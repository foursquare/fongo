package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.mongodb.DBObject;

public class ExpressionParser {

  public final static Pattern DOT_PATTERN = Pattern.compile("\\.");
  

  public Filter buildFilter(DBObject ref){
    AndFilter andFilter = new AndFilter();
    for (String key : ref.keySet()) {
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
  public final static String SIZE = "$size";
  public final static String NOT = "$not";
  public final static String OR = "$or";
  


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
          Option<Object> storedOption = getEmbeddedValue(key, o);
          if (storedOption.isEmpty()) {
            return false;
          } else {
            return compare(refExpression.get(command), storedOption.get());            
          }
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
  
  public <T> T typecast(String fieldName, Object obj, Class<T> clazz) {
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
  
  abstract class ConditionalOperatorFilterFactory extends BasicFilterFactory {

    public ConditionalOperatorFilterFactory(String command) {
      super(command);
    }
    final boolean compare(Object queryValue, Object storedValue) {
      if (storedValue instanceof List){
        for (Object aValue : (List)storedValue){
          if (aValue != null && singleCompare(queryValue, aValue)){
            return true;
          }
        }
        return false;
      } else {
        return storedValue != null && singleCompare(queryValue, storedValue);        
      }
    }
    abstract boolean singleCompare(Object queryValue, Object storedValue);
  }
  
  @SuppressWarnings("all")
  List<FilterFactory> filterFactories = Arrays.<FilterFactory>asList(
      new ConditionalOperatorFilterFactory(GTE){
        boolean singleCompare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) <= 0;
      }},
      new ConditionalOperatorFilterFactory(LTE){
        boolean singleCompare(Object queryValue, Object storedValue) {
            return compareObjects(queryValue, storedValue) >= 0;
      }},
      new ConditionalOperatorFilterFactory(GT){
        boolean singleCompare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) < 0;
      }},
      new ConditionalOperatorFilterFactory(LT){
        boolean singleCompare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) > 0;
      }},
      new BasicCommandFilterFactory(NE){
        public Filter createFilter(final String key, final DBObject refExpression) {
          return new Filter(){
            public boolean apply(DBObject o) {
              Object queryValue = refExpression.get(command);
              Option<Object> storedOption = getEmbeddedValue(key, o);
              if (storedOption.isEmpty()) {
                return true;
              } else {
                Object storedValue = storedOption.get();
                if (storedValue instanceof List){
                  for (Object aValue : (List)storedValue){
                    if (queryValue.equals(aValue)){
                      return false;
                    }
                  }
                  return true;
                } else {
                  return !queryValue.equals(storedValue);            
                }
              }
          }};
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
              Option<Object> storedOption = getEmbeddedValue(key, o);
              return typecast(command + " clause", refExpression.get(command), Boolean.class) == storedOption.isFull();
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
      new InFilterFactory(NIN, false),
      new BasicFilterFactory(SIZE){
        boolean compare(Object queryValue, Object storedValue) {
          Integer size = typecast(command + " clause", queryValue, Integer.class);
          List storedList = typecast("value", storedValue, List.class);
          return storedList != null && storedList.size() == size;
      }}
  );
  
  public Option<Object> getEmbeddedValue(String key, DBObject dbo) {
    String[] path = DOT_PATTERN.split(key);
    String subKey = path[0];
    for (int i = 0; i < path.length - 1; i++){
      Object value = dbo.get(subKey);
      if (value instanceof DBObject){
        dbo = (DBObject) value;
      } else {
        return Option.None;
      }
      subKey = path[i + 1];
    }
    if (dbo.containsField(subKey)) {
      return new Some<Object>(dbo.get(subKey));      
    } else {
      return Option.None;
    }
  }

  private Filter buildExpressionFilter(final String key, final Object expression) {
    if (key == OR) {
      List<DBObject> queryList = typecast(key + " operator", expression, List.class);
      OrFilter orFilter = new OrFilter();
      for (DBObject query : queryList) {
        orFilter.addFilter(buildFilter(query));
      }
      return orFilter;
    } else if (expression instanceof DBObject) {
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
    } else if (expression instanceof Pattern) {
      final Pattern pattern = (Pattern) expression;
      return new Filter(){
        public boolean apply(DBObject o) {
          Option<Object> storedOption = getEmbeddedValue(key, o);
          if (storedOption.isEmpty()){
            return false;
          } else {
            Object storedValue = storedOption.get();
            if (storedValue == null) {
              return false;
            } else if (storedValue instanceof List) {
              for (Object aValue : (List)storedValue) {
                if (aValue instanceof CharSequence){
                  if (pattern.matcher((CharSequence)aValue).find()){
                    return true;
                  }
                }
              }
              return false;
            } else {
              if (storedValue instanceof CharSequence){
                return pattern.matcher((CharSequence)storedValue).find();
              }
              return false;
            }
          }
        }
      };
    } else {
      return new Filter(){
        public boolean apply(DBObject o) {
          Option<Object> storedOption = getEmbeddedValue(key, o);
          if (storedOption.isEmpty()){
            return false;
          } else {
            Object storedValue = storedOption.get();
            if (storedValue instanceof List) {
              return ((List)storedValue).contains(expression);
            } else {
              return expression.equals(storedValue);            
            }
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

  static abstract class ConjunctionFilter implements Filter {
    
    List<Filter> filters = new ArrayList<Filter>();

    public void addFilter(Filter filter) {
      filters.add(filter);
    }
    
  }
  
  static class AndFilter extends ConjunctionFilter {
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
  
  static class OrFilter extends ConjunctionFilter {
    @Override
    public boolean apply(DBObject o) {
      for (Filter f : filters) {
        if (f.apply(o)){
          return true;
        }
      }
      return false;
    }
  }

}
