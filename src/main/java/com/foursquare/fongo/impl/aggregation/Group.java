package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.UpdateEngine;
import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

    @Override
    public String toString() {
      return "Mapping{" +
          "key=" + key +
          ", collection=" + collection +
          ", result=" + result +
          '}';
    }
  }

  private Group() {
  }

  public DBCollection apply(DBCollection coll, DBObject object) {
    DBObject group = (DBObject) object.get(getKeyword());

    Object id = ((DBObject) object.get(getKeyword())).removeField("_id");
    LOG.debug("group() for _id : {}", id);
    // Try to group in the mapping.
    Map<DBObject, Mapping> mapping = createMapping(coll, id);

    for (Map.Entry<String, Object> entry : ((Set<Map.Entry<String, Object>>) group.toMap().entrySet())) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (value instanceof DBObject) {
        DBObject objectValue = (DBObject) value;
        for (Map.Entry<DBObject, Mapping> entryMapping : mapping.entrySet()) {
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
          } else if (objectValue.containsField("$addToSet")) {
            result = pushAddToSet(workColl, objectValue.get("$addToSet"), true);
          } else if (objectValue.containsField("$push")) {
            result = pushAddToSet(workColl, objectValue.get("$push"), false);
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
    coll = dropAndInsert(coll, new ArrayList<DBObject>());

    // Extract from mapping to do the result.
    for (Map.Entry<DBObject, Mapping> entry : mapping.entrySet()) {
      coll.insert(entry.getValue().result);
      entry.getValue().collection.drop();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("group() : {} result : {}", object, coll.find().toArray());
    }
    return coll;
  }

  /**
   * Create mapping. Group result with a 'key'.
   *
   * @param coll collection to be mapped
   * @param id   id of the group
   * @return a (Criteria, Mapping) for the id.
   */
  private Map<DBObject, Mapping> createMapping(DBCollection coll, Object id) {
    Map<DBObject, Mapping> mapping = new HashMap<DBObject, Mapping>();
    List<DBObject> objects = coll.find().toArray();
    for (DBObject dbObject : objects) {
      DBObject criteria = criteriaForId(id, dbObject);
      if (!mapping.containsKey(criteria)) {
        // Return all object we can group
        List<DBObject> newCollection = coll.find(criteria).toArray();
        // Delete them from collection (optim for laaaaaarge collection)
        for (DBObject o : newCollection) {
          coll.remove(new BasicDBObject("_id", o.get("_id")));
        }
        // Generate key
        DBObject key = keyForId(id, dbObject);
        // Save into mapping
        mapping.put(criteria, new Mapping(key, createAndInsert(newCollection), Util.clone(key)));
        LOG.trace("createMapping() new criteria : {}", criteria);
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
      //ex: { "state" : "$state" , "city" : "$city"}
      DBObject subKey = new BasicDBObject();
      for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) ((DBObject) id).toMap().entrySet()) {
        subKey.put(entry.getKey(), Util.extractField(dbObject, fieldName(entry.getValue()))); // TODO : hierarchical, like "state" : {bar:"$foo"}
      }
      result.put("_id", subKey);
    } else if (id != null) {
      String field = fieldName(id);
      result.put("_id", Util.extractField(dbObject, field));
    } else {
      result.put("_id", null);
    }
    LOG.debug("keyForId() id:{}, dbObject:{}, result:{}", id, dbObject, result);
    return result;
  }

  private DBObject criteriaForId(Object id, DBObject dbObject) {
    DBObject result = new BasicDBObject();
    if (id instanceof DBObject) {
      for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) ((DBObject) id).toMap().entrySet()) {
        result.put(entry.getKey(), Util.extractField(dbObject, fieldName(entry.getValue()))); // TODO : hierarchical, like "state" : {bar:"$foo"}
      }
    } else if (id != null) {
      String field = fieldName(id);
      result.put(field, Util.extractField(dbObject, field));
    }
    LOG.debug("criteriaForId() id:{}, dbObject:{}, result:{}", id, dbObject, result);
    return result;
  }

  private String fieldName(Object name) {
    String field = name.toString();
    if (name instanceof String) {
      if (name.toString().startsWith("$")) {
        field = name.toString().substring(1);
      }
    }
    return field;
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
   * @param coll  grouped collection to make the avg
   * @param value field to be averaged.
   * @return the average of the collection.
   */
  private Double avg(DBCollection coll, Object value) {
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
   * Return the first or the last of a collection.
   *
   * @param coll
   * @param value fieldname for searching.
   * @return
   */
  private Object firstlast(DBCollection coll, Object value, boolean first) {
    LOG.debug("first({})/last({}) on {}", first, !first, value);
    Object result = null;
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      DBCursor cursor = coll.find();
      while (cursor.hasNext()) {
        result = Util.extractField(cursor.next(), field);
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
   * Return the first or the last of a collection.
   *
   * @param coll
   * @param value fieldname for searching.
   * @return
   */
  private BasicDBList pushAddToSet(DBCollection coll, Object value, boolean uniqueness) {
    LOG.debug("pushAddToSet() on {}", value);
    BasicDBList result = null;
    if (value.toString().startsWith("$")) {
      result = new BasicDBList();
      String field = value.toString().substring(1);
      DBCursor cursor = coll.find();
//      UpdateEngine updateEngine = new UpdateEngine();
//      for (DBObject objectColl : workColl.find().toArray()) {
//
//        DBObject push = new BasicDBObject("$push", )
//        updateEngine.doUpdate(entryMapping.getValue().result, objectValue);
//      }

      while (cursor.hasNext()) {
        Object fieldValue = Util.extractField(cursor.next(), field);
        if (!uniqueness || !result.contains(fieldValue)) {
          result.add(fieldValue);
        }
      }
    } else {
      LOG.error("Sorry, doesn't know what to do...");
    }

    LOG.debug("pushAddToSet() on {}, result : {}", value, result);
    return result;
  }

  /**
   * Return the min or the max of a collection.
   *
   * @param coll
   * @param value
   * @param valueComparable 0 for equals, -1 for min, +1 for max
   * @return
   */
  private Object minmax(DBCollection coll, Object value, int valueComparable) {
    if (value.toString().startsWith("$")) {
      String field = value.toString().substring(1);
      DBCursor cursor = coll.find();
      Comparable comparable = null;
      while (cursor.hasNext()) {
        DBObject object = cursor.next();
        if (Util.containsField(object, field)) {
          if (comparable == null) {
            comparable = Util.extractField(object, field);
          } else {
            Comparable other = Util.extractField(object, field);
            if (comparable.compareTo(other) == valueComparable) {
              comparable = other;
            }
          }
        }
      }
      return comparable;
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
