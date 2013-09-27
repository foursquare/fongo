package com.foursquare.fongo.impl;

import com.foursquare.fongo.FongoException;
import com.foursquare.fongo.impl.geo.GeoUtil;
import com.foursquare.fongo.impl.geo.LatLong;
import com.mongodb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionParser {
  final static Logger LOG = LoggerFactory.getLogger(ExpressionParser.class);

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
  public final static String AND = "$and";
  public final static String REGEX = "$regex";
  public final static String REGEX_OPTIONS = "$options";
  public final static String NEAR = "$near";
  public final static String NEAR_SPHERE = "$nearSphere";
  public final static String MAX_DISTANCE = "$maxDistance";
  public final static String ELEM_MATCH = QueryOperators.ELEM_MATCH;

  // TODO : http://docs.mongodb.org/manual/reference/operator/query-geospatial/
  // TODO : http://docs.mongodb.org/manual/reference/operator/geoWithin/#op._S_geoWithin
  // TODO : http://docs.mongodb.org/manual/reference/operator/geoIntersects/
  public final static String TYPE = "$type";

  public class ObjectComparator implements Comparator {
    private final int asc;

    ObjectComparator(boolean asc) {
      this.asc = asc ? 1 : -1;
    }

    @Override
    public int compare(Object o1, Object o2) {
      return asc * compareObjects(o1, o2);
    }
  }

  public Filter buildFilter(DBObject ref) {
    AndFilter andFilter = new AndFilter();
    if (ref != null) {
      for (String key : ref.keySet()) {
        Object expression = ref.get(key);
        andFilter.addFilter(buildExpressionFilter(key, expression));
      }
    }
    return andFilter;
  }

  /**
   * Only build the filter for this keys.
   *
   * @param ref  query for filter.
   * @param keys must match to build the filter.
   * @return
   */
  public Filter buildFilter(DBObject ref, Collection<String> keys) {
    AndFilter andFilter = new AndFilter();
    for (String key : ref.keySet()) {
      if (keys.contains(key)) {
        Object expression = ref.get(key);
        andFilter.addFilter(buildExpressionFilter(key, expression));
      }
    }
    return andFilter;
  }

  interface FilterFactory {
    public boolean matchesCommand(DBObject refExpression);

    public Filter createFilter(List<String> path, DBObject refExpression);
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
    public Filter createFilter(final List<String> path, final DBObject refExpression) {
      return new Filter() {
        public boolean apply(DBObject o) {
          List<Object> storedList = getEmbeddedValues(path, o);
          if (storedList.isEmpty()) {
            return false;
          } else {
            for (Object storedValue : storedList) {
              if (compare(refExpression.get(command), storedValue)) {
                return true;
              }
            }
            return false;
          }
        }
      };
    }

    abstract boolean compare(Object queryValue, Object storedValue);

  }

  @SuppressWarnings("all")
  private final class InFilterFactory extends BasicCommandFilterFactory {

    private final boolean direction;

    public InFilterFactory(String command, boolean direction) {
      super(command);
      this.direction = direction;
    }

    @Override
    public Filter createFilter(final List<String> path, final DBObject refExpression) {
      Collection queryList = typecast(command + " clause", refExpression.get(command), Collection.class);
      final Set querySet = new HashSet(queryList);
      return new Filter() {
        public boolean apply(DBObject o) {
          List<Object> storedList = getEmbeddedValues(path, o);
          if (storedList.isEmpty()) {
            return !direction;
          } else {
            for (Object storedValue : storedList) {
              if (compare(refExpression.get(command), storedValue, querySet) == direction) {
                return direction;
              }
            }
            return !direction;
          }
        }
      };
    }

    boolean compare(Object queryValueIgnored, Object storedValue, Set querySet) {
      if (storedValue instanceof List) {
        for (Object valueItem : (List) storedValue) {
          if (querySet.contains(valueItem)) return direction;
        }
        return !direction;
      } else {
        return !(direction ^ querySet.contains(storedValue));
      }
    }
  }

  private final class NearCommandFilterFactory extends BasicCommandFilterFactory {

    final boolean spherical;

    public NearCommandFilterFactory(final String command, boolean spherical) {
      super(command);
      this.spherical = spherical;
    }

    // http://docs.mongodb.org/manual/reference/operator/near/#op._S_near
    @Override
    public Filter createFilter(final List<String> path, DBObject refExpression) {
      LOG.debug("path:{}, refExp:{}", path, refExpression);
      Number maxDistance = typecast(MAX_DISTANCE, refExpression.get(MAX_DISTANCE), Number.class);
      final List<LatLong> coordinates;
      if (refExpression.get(command) instanceof BasicDBList) {
        coordinates = GeoUtil.latLon(Collections.singletonList(command), refExpression);// typecast(command, refExpression.get(command), List.class);
      } else {
        DBObject dbObject = typecast(command, refExpression.get(command), DBObject.class);
        coordinates = GeoUtil.latLon(Arrays.asList("$geometry", "coordinates"), dbObject);
      }
      return createNearFilter(path, coordinates, maxDistance, spherical);
    }
  }

  public <T> T typecast(String fieldName, Object obj, Class<T> clazz) {
    try {
      return clazz.cast(obj);
    } catch (Exception e) {
      throw new FongoException(fieldName + " expected to be of type " + clazz.getName() + " but is " + (obj != null ? obj.getClass() : "null") + " toString:" + obj);
    }
  }

  private void enforce(boolean check, String message) {
    if (!check) {
      throw new FongoException(message);
    }
  }

  abstract class ConditionalOperatorFilterFactory extends BasicFilterFactory {

    public ConditionalOperatorFilterFactory(String command) {
      super(command);
    }

    final boolean compare(Object queryValue, Object storedValue) {
      if (storedValue instanceof List) {
        for (Object aValue : (List) storedValue) {
          if (aValue != null && singleCompare(queryValue, aValue)) {
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
      new ConditionalOperatorFilterFactory(GTE) {
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue);
          return result != null && result.intValue() <= 0;
        }
      },
      new ConditionalOperatorFilterFactory(LTE) {
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue);
          return result != null && result.intValue() >= 0;
        }
      },
      new ConditionalOperatorFilterFactory(GT) {
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue);
          return result != null && result.intValue() < 0;
        }
      },
      new ConditionalOperatorFilterFactory(LT) {
        boolean singleCompare(Object queryValue, Object storedValue) {
          Integer result = compareObjects(queryValue, storedValue);
          return result != null && result.intValue() > 0;
        }
      },
      new BasicCommandFilterFactory(NE) {
        public Filter createFilter(final List<String> path, final DBObject refExpression) {
          return new Filter() {
            public boolean apply(DBObject o) {
              Object queryValue = refExpression.get(command);
              List<Object> storedList = getEmbeddedValues(path, o);
              if (storedList.isEmpty()) {
                return true;
              } else {
                for (Object storedValue : storedList) {
                  if (storedValue instanceof List) {
                    for (Object aValue : (List) storedValue) {
                      if (queryValue.equals(aValue)) {
                        return false;
                      }
                    }
                  } else {
                    if (queryValue.equals(storedValue)) {
                      return false;
                    }
                  }
                }
                return true;
              }
            }
          };
        }
      },
      new BasicFilterFactory(ALL) {
        boolean compare(Object queryValue, Object storedValue) {
          Collection queryList = typecast(command + " clause", queryValue, Collection.class);
          List storedList = typecast("value", storedValue, List.class);
          if (storedList == null) {
            return false;
          }

          for (Object queryObject : queryList) {
            if (queryObject instanceof Pattern) {
              if (!listContainsPattern(storedList, (Pattern) queryObject)) {
                return false;
              }
            } else {
              if (!storedList.contains(queryObject)) {
                return false;
              }
            }
          }

          return true;
        }
      },
      new BasicFilterFactory(ELEM_MATCH) {
        boolean compare(Object queryValue, Object storedValue) {
          DBObject query = typecast(command + " clause", queryValue, DBObject.class);
          List storedList = typecast("value", storedValue, List.class);
          if (storedList == null) {
            return false;
          }

          Filter filter = buildFilter(query);
          for(Object object : storedList) {
              if(filter.apply((DBObject) object)) {
                  return true;
              }
          }

          return false;
        }
      },
      new BasicCommandFilterFactory(EXISTS) {
        public Filter createFilter(final List<String> path, final DBObject refExpression) {
          return new Filter() {
            public boolean apply(DBObject o) {
              List<Object> storedOption = getEmbeddedValues(path, o);
              return typecast(command + " clause", refExpression.get(command), Boolean.class) == !storedOption.isEmpty();
            }
          };
        }
      },
      new BasicFilterFactory(MOD) {

        boolean compare(Object queryValue, Object storedValue) {
          List<Integer> queryList = typecast(command + " clause", queryValue, List.class);
          enforce(queryList.size() == 2, command + " clause must be a List of size 2");
          int modulus = queryList.get(0);
          int expectedValue = queryList.get(1);
          return (storedValue != null) && (typecast("value", storedValue, Number.class).longValue()) % modulus == expectedValue;
        }
      },
      new InFilterFactory(IN, true),
      new InFilterFactory(NIN, false),
      new BasicFilterFactory(SIZE) {
        boolean compare(Object queryValue, Object storedValue) {
          Integer size = typecast(command + " clause", queryValue, Integer.class);
          List storedList = typecast("value", storedValue, List.class);
          return storedList != null && storedList.size() == size;
        }
      },
      new BasicCommandFilterFactory(REGEX) {
        @Override
        public Filter createFilter(final List<String> path, DBObject refExpression) {
          String flagStr = typecast(REGEX_OPTIONS, refExpression.get(REGEX_OPTIONS), String.class);
          int flags = parseRegexOptionsToPatternFlags(flagStr);
          final Pattern pattern = Pattern.compile(refExpression.get(this.command).toString(), flags);

          return createPatternFilter(path, pattern);
        }
      },
      new NearCommandFilterFactory(NEAR_SPHERE, true),
      new NearCommandFilterFactory(NEAR, false),
      new BasicCommandFilterFactory(TYPE) {
        @Override
        public Filter createFilter(final List<String> path, DBObject refExpression) {
          Number type = typecast(TYPE, refExpression.get(TYPE), Number.class);

          return createTypeFilter(path, type.intValue());
        }
      }
  );

  boolean objectMatchesPattern(Object obj, Pattern pattern) {
    if (obj instanceof CharSequence) {
      if (pattern.matcher((CharSequence) obj).find()) {
        return true;
      }
    }
    return false;
  }

  boolean listContainsPattern(List<Object> list, Pattern pattern) {
    for (Object obj : list) {
      if (objectMatchesPattern(obj, pattern)) {
        return true;
      }
    }
    return false;
  }


  /**
   * http://docs.mongodb.org/manual/reference/operator/type
   * <p/>
   * Type	Number
   * Double	1
   * String	2
   * Object	3
   * Array	4
   * Binary data	5
   * Undefined (deprecated)	6
   * Object id	7
   * Boolean	8
   * Date	9
   * Null	10
   * Regular Expression	11
   * JavaScript	13
   * Symbol	14
   * JavaScript (with scope)	15
   * 32-bit integer	16
   * Timestamp	17
   * 64-bit integer	18
   * Min key	255
   * Max key	127
   */
  boolean objectMatchesType(Object obj, int type) {
    switch (type) {
      case 1:
        return obj instanceof Double || obj instanceof Float;
      case 2:
        return obj instanceof CharSequence;
      case 3:
        return obj instanceof Object;
      case 4:
        return obj instanceof List;
      case 7:
        return obj instanceof ObjectId;
      case 8:
        return obj instanceof Boolean;
      case 9:
        return obj instanceof Date;
      case 10:
        return obj == null;
      case 11:
        return obj instanceof Pattern;
      case 16:
        return obj instanceof Integer;
      case 18:
        return obj instanceof Long;
    }
    return false;
  }

  public List<Object> getEmbeddedValues(List<String> path, DBObject dbo) {
    return getEmbeddedValues(path, 0, dbo);
  }

  public List<Object> getEmbeddedValues(String key, DBObject dbo) {
    return getEmbeddedValues(Util.split(key), 0, dbo);
  }

  public List<Object> extractDBRefValue(DBRefBase ref, String refKey) {
    if ("$id".equals(refKey)) {
      return Collections.singletonList(ref.getId());
    } else if ("$ref".equals(refKey)) {
      return Collections.<Object>singletonList(ref.getRef());
    } else if ("$db".equals(refKey)) {
      return Collections.<Object>singletonList(ref.getDB());
    } else return Collections.emptyList();
  }

  public List<Object> getEmbeddedValues(List<String> path, int startIndex, DBObject dbo) {
    String subKey = path.get(startIndex);
    if (path.size() > 1 && LOG.isDebugEnabled()) {
      LOG.debug("getEmbeddedValue looking for {} in {}", path, dbo);
    }

    for (int i = startIndex; i < path.size() - 1; i++) {
      Object value = dbo.get(subKey);
      if (value instanceof DBObject && !(value instanceof List)) {
        dbo = (DBObject) value;
      } else if (value instanceof List && Util.isPositiveInt(path.get(i + 1))) {
        BasicDBList newList = Util.wrap((List) value);
        dbo = newList;
      } else if (value instanceof List) {
        List<Object> results = new ArrayList<Object>();
        for (Object listValue : (List) value) {
          if (listValue instanceof DBObject) {
            List<Object> embeddedListValue = getEmbeddedValues(path, i + 1, (DBObject) listValue);
            results.addAll(embeddedListValue);
          } else if (listValue instanceof DBRefBase) {
            results.addAll(extractDBRefValue((DBRefBase) listValue, path.get(i + 1)));
          }
        }
        if (!results.isEmpty()) {
          return results;
        }
      } else if (value instanceof DBRefBase) {
        return extractDBRefValue((DBRefBase) value, path.get(i + 1));
      } else {
        return Collections.emptyList();
      }
      subKey = path.get(i + 1);
    }
    if (dbo.containsField(subKey)) {
      return Collections.singletonList((dbo.get(subKey)));
    } else {
      return Collections.emptyList();
    }
  }


  private Filter buildExpressionFilter(final String key, final Object expression) {
    return buildExpressionFilter(Util.split(key), expression);
  }


  private Filter buildExpressionFilter(final List<String> path, Object expression) {
    if (OR.equals(path.get(0))) {
      Collection<DBObject> queryList = typecast(path + " operator", expression, Collection.class);
      OrFilter orFilter = new OrFilter();
      for (DBObject query : queryList) {
        orFilter.addFilter(buildFilter(query));
      }
      return orFilter;
    } else if (AND.equals(path.get(0))) {
      Collection<DBObject> queryList = typecast(path + " operator", expression, Collection.class);
      AndFilter andFilter = new AndFilter();
      for (DBObject query : queryList) {
        andFilter.addFilter(buildFilter(query));
      }
      return andFilter;
    } else if (expression instanceof DBObject || expression instanceof Map) {
      DBObject ref = expression instanceof DBObject ? (DBObject) expression : new BasicDBObject((Map) expression);

      if (ref.containsField(NOT)) {
        return new NotFilter(buildExpressionFilter(path, ref.get(NOT)));
      } else {

        AndFilter andFilter = new AndFilter();
        int matchCount = 0;
        for (FilterFactory filterFactory : filterFactories) {
          if (filterFactory.matchesCommand(ref)) {
            matchCount++;
            andFilter.addFilter(filterFactory.createFilter(path, ref));
          }
        }
        if (matchCount == 0) {
          return simpleFilter(path, expression);
        }
        if (matchCount > 2) {
          throw new FongoException("Invalid expression for key " + path + ": " + expression);
        }
        return andFilter;
      }
    } else if (expression instanceof Pattern) {
      return createPatternFilter(path, (Pattern) expression);
    } else {
      return simpleFilter(path, expression);
    }
  }

  public Filter simpleFilter(final List<String> path, final Object expression) {
    return new Filter() {
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return (expression == null);
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue instanceof List) {
              if(expression instanceof List){
                if(storedValue.equals(expression)){
                  return true;
                }
              }
              if (((List) storedValue).contains(expression)) {
                return true;
              }
            } else {
              if (expression == null) {
                return (storedValue == null);
              }
              if (expression.equals(storedValue)) {
                return true;
              }
            }
          }
          return false;
        }

      }
    };
  }


  @SuppressWarnings("all")
  public Integer compareObjects(Object queryValue, Object storedValue) {
    LOG.debug("comparing {} and {}", queryValue, storedValue);

    if (queryValue instanceof DBObject && storedValue instanceof DBObject) {
      return compareDBObjects((DBObject) queryValue, (DBObject) storedValue);
    } else if (queryValue instanceof List && storedValue instanceof List) {
      List queryList = (List) queryValue;
      List storedList = (List) storedValue;
      return compareLists(queryList, storedList);
    } else {
      Comparable queryComp = typecast("query value", queryValue, Comparable.class);
      if(!(storedValue instanceof Comparable) && storedValue != null) {
        return null;
      }
      Comparable storedComp = typecast("stored value", storedValue, Comparable.class);
      if (storedComp == null) {
        return 1;
      }
      return queryComp.compareTo(storedComp);
    }
  }

  public int compareLists(List queryList, List storedList) {
    int sizeDiff = queryList.size() - storedList.size();
    if (sizeDiff != 0) {
      return sizeDiff;
    }
    for (int i = 0; i < queryList.size(); i++) {
      int compareValue = compareObjects(queryList.get(i), storedList.get(i));
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0;
  }

  private int compareDBObjects(DBObject db0, DBObject db1) {
    for (String key : db0.keySet()) {
      int compareValue = compareObjects(db0.get(key), db1.get(key));
      if (compareValue != 0) {
        return compareValue;
      }
    }
    return 0;
  }

  public Filter createPatternFilter(final List<String> path, final Pattern pattern) {
    return new Filter() {
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return false;
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue != null) {
              if (storedValue instanceof List) {
                if (listContainsPattern((List) storedValue, pattern)) {
                  return true;
                }
              } else if (objectMatchesPattern(storedValue, pattern)) {
                return true;
              }
            }
          }
          return false;
        }
      }
    };
  }

  // Take care of : https://groups.google.com/forum/?fromgroups=#!topic/mongomapper/MfRDh2vtCFg
  public Filter createNearFilter(final List<String> path, final List<LatLong> coordinates, final Number maxDistance, final boolean sphere) {
    return new Filter() {
      final LatLong coordinate = coordinates.get(0); // TODO(twillouer) try to get all coordinate.
      int limit = 100;

      public boolean apply(DBObject o) {
        if (limit <= 0) {
          return false;
        }
        boolean result = false;

        List<LatLong> storedOption = GeoUtil.latLon(path, o);
        if (!storedOption.isEmpty()) {
          if (maxDistance == null) {
            result = true;
          } else {
            for (LatLong point : storedOption) {

              double distance = GeoUtil.distanceInRadians(point, coordinate, sphere);
              LOG.debug("distance : {}", distance);
              result = distance < maxDistance.doubleValue();
              if (result) {
                break;
              }
            }
          }
        }
        if (result) {
          limit--;
        }
        return result;
      }
    };
  }

  public Filter createTypeFilter(final List<String> path, final int type) {
    return new Filter() {
      public boolean apply(DBObject o) {
        List<Object> storedOption = getEmbeddedValues(path, o);
        if (storedOption.isEmpty()) {
          return false;
        } else {
          for (Object storedValue : storedOption) {
            if (storedValue instanceof Collection) {
              for (Object object : (Collection) storedValue) {
                if (objectMatchesType(object, type)) {
                  return true;
                }
              }
            } else if (objectMatchesType(storedValue, type)) {
              return true;
            }
          }
          return false;
        }
      }
    };
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
        if (!f.apply(o)) {
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
        if (f.apply(o)) {
          return true;
        }
      }
      return false;
    }
  }

  public static final Filter AllFilter = new Filter() {
    @Override
    public boolean apply(DBObject o) {
      return true;
    }
  };

  public int parseRegexOptionsToPatternFlags(String flagString) {
    int flags = 0;
    for (int i = 0; flagString != null && i < flagString.length(); i++) {
      switch (flagString.charAt(i)) {
        case 'i':
          flags |= Pattern.CASE_INSENSITIVE;
          break;
        case 'x':
          flags |= Pattern.COMMENTS;
          break;
        case 'm':
          flags |= Pattern.MULTILINE;
          break;
        case 's':
          flags |= Pattern.DOTALL;
          break;
      }
    }
    return flags;
  }

  public ObjectComparator buildObjectComparator(boolean asc) {
    return new ObjectComparator(asc);
  }
}
