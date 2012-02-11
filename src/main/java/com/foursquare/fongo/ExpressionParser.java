package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  
  interface FilterFactory {
    public boolean matchesCommand(DBObject refExpression);
    public Filter createFilter(String key, DBObject refExpression);
  }
  
  abstract class BasicFilterFactory implements FilterFactory {

    private final String command;
    
    public BasicFilterFactory(final String command) {
      this.command = command;
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
  
  List<FilterFactory> filterFactories = Arrays.<FilterFactory>asList(
      new BasicFilterFactory(GTE){
        boolean compare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) <= 0;
      }},
      new BasicFilterFactory(LTE){
        boolean compare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) >= 0;
      }},
      new BasicFilterFactory(GT){
        boolean compare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) < 0;
      }},
      new BasicFilterFactory(LT){
        boolean compare(Object queryValue, Object storedValue) {
          return compareObjects(queryValue, storedValue) > 0;
      }},
      new BasicFilterFactory(NE){
        boolean compare(Object queryValue, Object storedValue) {
          return !queryValue.equals(storedValue);
      }}
  );

  private Filter buildExpressionFilter(final String key, final Object expression) {
    if (expression instanceof DBObject) {
      DBObject ref = (DBObject) expression;
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
    } else {
      return new Filter(){
        public boolean apply(DBObject o) {
          return expression.equals(o.get(key));
        }};
    }
  }

  private int compareObjects(Object queryValue, Object storedValue) {
    if (queryValue instanceof Comparable) {
      return ((Comparable)queryValue).compareTo(storedValue); 
    } else {
      throw new FongoException("can't compare values that don't implement java.lang.Comparable: " + queryValue);
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
