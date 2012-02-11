package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.DBObject;

public class ExpressionParser {

  public Filter buildFilter(DBObject ref){
    return buildFilter(ref, new AndFilter());
  }
  
  public Filter buildFilter(DBObject ref, AndFilter andFilter){
    for (String key : ref.keySet()) {
      if (key.startsWith("$")) {
        throw new RuntimeException(key + " not supported");
      }
      Object expression = ref.get(key);
      andFilter.addFilter(buildExpressionFilter(key, expression));
    }
    return andFilter;
  }
  
  public final static String LT = "$lt";
  public final static String LTE = "$lte";
  public final static String GT = "$gt";
  public final static String GTE = "$gte";
  public final static String NE = "$ne";
  public final static String ALL = "$all";
  public final static String EXISTS = "$exists";
  public final static String MOD = "$mod";
  public final static String IN = "$in";
  public final static String NIN = "$nin";
  public final static String NOT = "$not";
  


  interface FilterFactory {
    public boolean matchesCommand(DBObject refExpression);
    public Filter createFilter(String key, DBObject refExpression);
  }
  
  abstract class BasicCommandFilterFactory implements FilterFactory {

    public final String command;
    
    public BasicCommandFilterFactory(final String command) {
      this.command = command;
    }
    
    public boolean matchesCommand(DBObject refExpression) {
      return refExpression.containsField(command);
    }
  }
  
  abstract class BasicFilterFactory extends BasicCommandFilterFactory {
    public BasicFilterFactory(final String command) {
      super(command);
    }
    
    public boolean matchesCommand(DBObject refExpression) {
      return refExpression.containsField(command);
    }
    
    @Override
    public Filter createFilter(final String key, final DBObject refExpression) {
      return new Filter(){
        public boolean apply(DBObject o) {
          return compare(refExpression.get(command), o.get(key));
        }};
    }
    
    abstract boolean compare(Object queryValue, Object storedValue);
    
  }
  @SuppressWarnings("all")
  private final class InFilterFactory extends BasicFilterFactory {
    private Set querySet;
    private final boolean direction;
    
    public InFilterFactory(String command, boolean direction) {
      super(command);
      this.direction = direction;
    }

    @Override
    public boolean matchesCommand(DBObject ref) {
      Object commandValue = ref.get(command);
      if (commandValue != null){
        List queryList = typecast(command + " clause", commandValue, List.class);
        this.querySet = new HashSet(queryList);
        return true;
      }
      return false;
    }

    boolean compare(Object queryValueIgnored, Object storedValue) {
      if (storedValue instanceof List){
        for (Object valueItem : (List)storedValue){
          if (querySet.contains(valueItem)) return direction;
        }
        return !direction;
      } else {
        return !(direction ^ querySet.contains(storedValue));
      }
   }
  }
  
  private <T> T typecast(String fieldName, Object obj, Class<T> clazz) {
    try {
      return clazz.cast(obj);
    } catch (Exception e) {
      throw new FongoException(fieldName + " expected to be of type " + clazz.getName() + " but is " + obj);
    }
  }
  
  private void enforce(boolean check, String message){
    if (!check) {
      throw new FongoException(message);
    }
  }
  
  @SuppressWarnings("all")
  List<FilterFactory> filterFactories = Arrays.<FilterFactory>asList(
      new BasicFilterFactory(GTE){
        boolean compare(Object queryValue, Object storedValue) {
          return storedValue != null && compareObjects(queryValue, storedValue) <= 0;
      }},
      new BasicFilterFactory(LTE){
        boolean compare(Object queryValue, Object storedValue) {
          return storedValue != null && compareObjects(queryValue, storedValue) >= 0;
      }},
      new BasicFilterFactory(GT){
        boolean compare(Object queryValue, Object storedValue) {
          return storedValue != null && compareObjects(queryValue, storedValue) < 0;
      }},
      new BasicFilterFactory(LT){
        boolean compare(Object queryValue, Object storedValue) {
          return storedValue != null && compareObjects(queryValue, storedValue) > 0;
      }},
      new BasicFilterFactory(NE){
        boolean compare(Object queryValue, Object storedValue) {
          return !queryValue.equals(storedValue);
      }},
      new BasicFilterFactory(ALL){
        boolean compare(Object queryValue, Object storedValue) {
          List queryList = typecast(command + " clause", queryValue, List.class);
          List storedList = typecast("value", storedValue, List.class);
          return storedList != null && storedList.containsAll(queryList);
      }},
      new BasicCommandFilterFactory(EXISTS){
        public Filter createFilter(final String key, final DBObject refExpression) {
          return new Filter(){
            public boolean apply(DBObject o) {
              return typecast(command + " clause", refExpression.get(command), Boolean.class) == o.containsField(key) ;
          }};
      }},
      new BasicFilterFactory(MOD){
        
        boolean compare(Object queryValue, Object storedValue) {
          List<Integer> queryList = typecast(command + " clause", queryValue, List.class);
          enforce(queryList.size() == 2, command + " clause must be a List of size 2");
          int modulus = queryList.get(0);
          int expectedValue = queryList.get(1);
          return (storedValue != null) && (typecast("value", storedValue, Number.class).longValue()) % modulus == expectedValue;
      }},
      new InFilterFactory(IN, true),
      new InFilterFactory(NIN, false)
  );

  private Filter buildExpressionFilter(final String key, final Object expression) {
    if (expression instanceof DBObject) {
      DBObject ref = (DBObject) expression;
      Object notExpression = ref.get(NOT);
      if (notExpression != null) {
        return new NotFilter(buildExpressionFilter(key, notExpression));
      } else {
        AndFilter andFilter = new AndFilter();
        int matchCount = 0;
        for (FilterFactory filterFactory : filterFactories){
          if (filterFactory.matchesCommand(ref)) {
            matchCount++;
            andFilter.addFilter(filterFactory.createFilter(key, ref));
          }
        }
        if (matchCount == 0 || matchCount > 2){
          throw new FongoException("Invalid expression for key " + key + ": " + expression);
        }
        return andFilter;
      }
    } else {
      return new Filter(){
        public boolean apply(DBObject o) {
          Object storedValue = o.get(key);
          if (storedValue instanceof List) {
            return ((List)storedValue).contains(expression);
          } else {
            return expression.equals(storedValue);            
          }
        }};
    }
  }

  @SuppressWarnings("all")
  private int compareObjects(Object queryValue, Object storedValue) {
    Comparable queryComp = typecast("query value", queryValue, Comparable.class);
    Comparable storedComp = typecast("stored value", storedValue, Comparable.class);
    return queryComp.compareTo(storedComp);
  }
  
  static class NotFilter implements Filter {
    private final Filter filter;
    public NotFilter(Filter filter) {
      this.filter = filter;
    }
    public boolean apply(DBObject o) {
      return !filter.apply(o);
    }
    
  }

  static class AndFilter implements Filter {

    private List<Filter> filters = new ArrayList<Filter>();

    public void addFilter(Filter filter) {
      filters.add(filter);
    }
    @Override
    public boolean apply(DBObject o) {
      for (Filter f : filters) {
        if (!f.apply(o)){
          return false;
        }
      }
      return true;
    }
    
  }

}
