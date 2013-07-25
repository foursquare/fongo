package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.util.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@see http://docs.mongodb.org/manual/reference/aggregation/group/}
 */
@ThreadSafe
public class Group extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Group.class);

  public static final Group INSTANCE = new Group();


  static class Mapping {
    private final DBObject key;

    private final DBCollection collection;

    private final DBObject result;

    public Mapping(DBObject key, DBCollection collection, DBObject result) {
      this.key = key;
      this.collection = collection;
      this.result = result;
    }
  }

  private Group() {
  }

  // $group : { _id : "0", "$max":"$date" }
  // $group: { _id: "$department", average: { $avg: "$amount" } }
  // $group : { _id : { state : "$state", city : "$city" },
  public DBCollection apply(DBCollection coll, DBObject object) {
    DBObject group = (DBObject) object.get(getKeyword());

    Object id = ((DBObject) object.get(getKeyword())).get("_id");
    // Try to group in the mapping.
    Map<DBObject, Mapping> mapping = createMapping(coll, id);

    for (Map.Entry<String, Object> entry : ((Set<Map.Entry<String, Object>>) group.toMap().entrySet())) {
      String key = entry.getKey();
      if (!key.equals("_id")) {
        Object value = entry.getValue();
        if (value instanceof DBObject) {
          DBObject objectValue = (DBObject) value;
          for (Map.Entry<DBObject, Mapping> entryMapping : mapping.entrySet()) {
            LOG.debug("group() key:{}", entryMapping.getKey());
            DBCollection workColl = entryMapping.getValue().collection;
            Object result = null;
            boolean nullForced = false;
            if (objectValue.containsField("$min")) {
              result = minmax(workColl, objectValue.get("$min"), 1);
            } else if (objectValue.containsField("$max")) {
              result = minmax(workColl, objectValue.get("$max"), -1);
            } else if (objectValue.containsField("$last")) {
              result = firstlast(workColl, objectValue.get("$last"), false);
              nullForced = true;
            } else if (objectValue.containsField("$first")) {
              result = firstlast(workColl, objectValue.get("$first"), true);
              nullForced = true;
            } else if (objectValue.containsField("$avg")) {
              result = avg(workColl, objectValue.get("$avg"));
            } else if (objectValue.containsField("$sum")) {
              result = sum(workColl, objectValue.get("$sum"));
            }

            if (result != null || nullForced) {
              LOG.debug("_id:{}, key:{}, result:{}", entryMapping.getKey(), key, result);
              entryMapping.getValue().result.put(key, result);
            } else {
              LOG.warn("result is null for entry {}", entry);
            }
          }
        }
      }
    }
    coll = dropAndInsert(coll, new ArrayList<DBObject>());
    for (Map.Entry<DBObject, Mapping> entry : mapping.entrySet()) {
      coll.insert(entry.getValue().result);
    }

    LOG.debug("group() : {} result : {}", object, mapping);
    return coll;
  }

  /**
   * Create mapping. Group result with a 'key'.
   *
   * @param coll
   * @param id
   * @return
   */
  private Map<DBObject, Mapping> createMapping(DBCollection coll, Object id) {
    Map<DBObject, Mapping> mapping = new HashMap<DBObject, Mapping>();
    List<DBObject> objects = coll.find().toArray();
    for (DBObject dbObject : objects) {
      DBObject key = keyForId(id, dbObject);
      if (!mapping.containsKey(key)) {
        mapping.put(key, new Mapping(key, createAndInsert(coll.find(criteriaForId(id, dbObject)).toArray()), Util.clone(key)));
      }
    }
    return mapping;
  }

  /**
   * Get the key from the "_id".
   *
   * @param id
   * @param dbObject
   * @return
   */
  private DBObject keyForId(Object id, DBObject dbObject) {
    DBObject result = new BasicDBObject();
    if (id instanceof DBObject) {
      // TODO
    } else if (id != null) {
      String field = id.toString();
      if (id instanceof String) {
        if (id.toString().startsWith("$")) {
          field = id.toString().substring(1);
        }
      }
      result.put("_id", dbObject.get(field));
    } else {
      result.put("_id", null);
    }
    LOG.debug("keyForId() id:{}, dbObject:{}, result:{}", id, dbObject, result);
    return result;
  }

  private DBObject criteriaForId(Object id, DBObject dbObject) {
    DBObject result = new BasicDBObject();
    if (id instanceof DBObject) {
      // TODO
    } else if (id != null) {
      String field = id.toString();
      if (id instanceof String) {
        if (id.toString().startsWith("$")) {
          field = id.toString().substring(1);
        }
      }
      result.put(field, dbObject.get(field));
    }
    LOG.debug("criteriaForId() id:{}, dbObject:{}, result:{}", id, dbObject, result);
    return result;
  }

  /**
   * {@see http://docs.mongodb.org/manual/reference/aggregation/sum/#grp._S_sum}
   *
   * @param coll
   * @param value
   * @return
   */
  private Object sum(DBCollection coll, Object value) {
    Number result = null;
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      List<DBObject> objects = coll.find(null, new BasicDBObject(field, 1).append("_id", 0)).toArray();
      for (DBObject object : objects) {
        LOG.debug("sum object {} ", object);
        if (Util.containsField(object, field)) {
          if (result == null) {
            result = Util.extractField(object, field);
          } else {
            Number other = Util.extractField(object, field);
            result = addWithSameType(result, other);
          }
        }
      }
    } else {
      Number iValue = (Number) value;
      // TODO : handle null value ?
      result = coll.count() * iValue.doubleValue();
    }
    return result;
  }

  /**
   * {@see http://docs.mongodb.org/manual/reference/aggregation/avg/#grp._S_avg}
   *
   * @param coll
   * @param value
   * @return
   */
  private Object avg(DBCollection coll, Object value) {
    Number result = null;
    long count = 1;
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      List<DBObject> objects = coll.find(null, new BasicDBObject(field, 1).append("_id", 0)).toArray();
      for (DBObject object : objects) {
        LOG.debug("avg object {} ", object);

        if (Util.containsField(object, field)) {
          if (result == null) {
            result = Util.extractField(object, field);
          } else {
            count++;
            Number other = Util.extractField(object, field);
            result = addWithSameType(result, other);
          }
        }
      }
    } else {
      LOG.error("Sorry, doesn't know what to do...");
      return null;
    }
    // Always return double.
    return (result.doubleValue() / (double) count);
  }

  /**
   * @param coll
   * @param value
   * @return
   */
  private Object firstlast(DBCollection coll, Object value, boolean first) {
    LOG.debug("first({})/last({}) on {}", first, !first, value);
    Object result = null;
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      List<DBObject> objects = coll.find(null, new BasicDBObject(field, 1).append("_id", 0)).toArray();
      for (DBObject object : objects) {
        result = Util.extractField(object, field);
        if (first) {
          break;
        }
      }
    } else {
      LOG.error("Sorry, doesn't know what to do...");
    }

    LOG.debug("first({})/last({}) on {}, result : {}", first, !first, value, result);
    return result;
  }

  /**
   * @param coll
   * @param value
   * @param valueComparable 0 for equals, -1 for min, +1 for max
   * @return
   */
  private Object minmax(DBCollection coll, Object value, int valueComparable) {
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      List<DBObject> objects = coll.find(null, new BasicDBObject(field, 1).append("_id", 0)).toArray();
      Comparable compable = null;
      for (DBObject object : objects) {
        LOG.debug("minmax object {} ", object);
        if (Util.containsField(object, field)) {
          if (compable == null) {
            compable = Util.extractField(object, field);
          } else {
            Comparable other = Util.extractField(object, field);
            LOG.trace("minmax {} vs {}", compable, other);
            if (compable.compareTo(other) == valueComparable) {
              compable = other;
            }
          }
        }
      }
      return compable;
    } else {
      LOG.error("Sorry, doesn't know what to do...");
    }
    return null;
  }

  /**
   * Add two number in the same type.
   *
   * @param result
   * @param other
   * @return
   */
  private Number addWithSameType(Number result, Number other) {
    if (result instanceof Float) {
      result = Float.valueOf(result.floatValue() + other.floatValue());
    } else if (result instanceof Double) {
      result = Double.valueOf(result.doubleValue() + other.doubleValue());
    } else if (result instanceof Integer) {
      result = Integer.valueOf(result.intValue() + other.intValue());
    } else if (result instanceof Long) {
      result = Long.valueOf(result.longValue() + other.longValue());
    } else {
      LOG.warn("type of field not handled for sum : {}", result.getClass());
    }
    return result;
  }

  private Number returnSameType(Number type, Number other) {
    if (type instanceof Float) {
      return Float.valueOf(other.floatValue());
    } else if (type instanceof Double) {
      return Double.valueOf(other.doubleValue());
    } else if (type instanceof Integer) {
      return Integer.valueOf(other.intValue());
    } else if (type instanceof Long) {
      return Long.valueOf(other.longValue());
    } else {
      LOG.warn("type of field not handled for sum : {}", type.getClass());
    }
    return other;
  }

  @Override
  public String getKeyword() {
    return "$group";
  }

}
