package com.foursquare.fongo.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 22/07/13
 */
public class Aggregator {
  private static final Logger LOG = LoggerFactory.getLogger(Aggregator.class);

  private final FongoDB fongoDB;
  private final FongoDBCollection fongoDBCollection;
  private final List<DBObject> pipeline;

  public Aggregator(FongoDB fongoDB, FongoDBCollection coll, List<DBObject> pipeline) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.pipeline = pipeline;
  }

  /**
   * @return null if error.
   */
  public List<DBObject> computeResult() {
    DBCollection coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(this.fongoDBCollection.find().toArray());

    for (DBObject object : pipeline) {
      if (object.containsField("$match")) {
        coll = match(coll, object);
      } else if (object.containsField("$project")) {
        coll = project(coll, object);
      } else if (object.containsField("$group")) {
        coll = group(coll, object);
      } else if (object.containsField("$sort")) {
        List<DBObject> objects = coll.find().sort((DBObject) object.get("$sort")).toArray();
        coll = dropAndInsert(coll, objects);
      } else if (object.containsField("$limit")) {
        List<DBObject> objects = coll.find().limit(((Number) object.get("$limit")).intValue()).toArray();
        coll = dropAndInsert(coll, objects);
      } else if (object.containsField("$skip")) {
        List<DBObject> objects = coll.find().skip(((Number) object.get("$skip")).intValue()).toArray();
        coll = dropAndInsert(coll, objects);
      }
    }

    List<DBObject> result = coll.find().toArray();
    coll.drop();

    LOG.debug("computeResult : {}", result);

    return result;
  }

  /**
   * Simple version of : {@see http://docs.mongodb.org/manual/reference/aggregation/group/#pipe._S_group}
   *
   * @param coll
   * @param object
   * @return
   */
  private DBCollection group(DBCollection coll, DBObject object) {
    DBObject group = (DBObject) object.get("$group");
    // $group : { _id : "0", "$max":"$date" }
    // $group: { _id: "$department", average: { $avg: "$amount" } }
    List<DBObject> objects = new ArrayList<DBObject>();
    for (Map.Entry<String, Object> entry : ((Set<Map.Entry<String, Object>>) group.toMap().entrySet())) {
      String key = entry.getKey();
      if (!key.equals("_id")) {
        Object value = entry.getValue();
        if (value instanceof DBObject) {
          DBObject objectValue = (DBObject) value;
          Object result = null;
          boolean nullForced = false;
          if (objectValue.containsField("$min")) {
            result = minmax(coll, objectValue.get("$min"), 1);
          } else if (objectValue.containsField("$max")) {
            result = minmax(coll, objectValue.get("$max"), -1);
          } else if (objectValue.containsField("$last")) {
            result = firstlast(coll, objectValue.get("$last"), false);
            nullForced = true;
          } else if (objectValue.containsField("$first")) {
            result = firstlast(coll, objectValue.get("$first"), true);
            nullForced = true;
          } else if (objectValue.containsField("$avg")) {
            result = avg(coll, objectValue.get("$avg"));
          } else if (objectValue.containsField("$sum")) {
            result = sum(coll, objectValue.get("$sum"));
          }

          if (result != null || nullForced) {
            objects.add(new BasicDBObject(key, result));
            LOG.debug("key:{}, result:{}", key, result);
          } else {
            LOG.warn("result is null for entry {}", entry);
          }
        }
      }
    }
    coll = dropAndInsert(coll, objects);
    LOG.debug("group : {} result : {}", object, objects);
    return coll;
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
          }
        }
      }
    } else {
      int iValue = Integer.parseInt(value.toString());
      // TODO : handle null value ?
      result = coll.count() * iValue;
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
            if (result instanceof Float) {
              result = Float.valueOf(result.floatValue() + other.floatValue());
            } else if (result instanceof Double) {
              result = Double.valueOf(result.doubleValue() + other.doubleValue());
            } else if (result instanceof Integer) {
              result = Integer.valueOf(result.intValue() + other.intValue());
            } else if (result instanceof Long) {
              result = Long.valueOf(result.longValue() + other.longValue());
            } else {
              LOG.warn("type of field not handled for avg : {}", result.getClass());
            }
          }
        }
      }
    } else {
      LOG.error("Sorry, doesn't know what to do...");
      return null;
    }
    return result.doubleValue() / count;
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
        ;
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
   * {@see http://docs.mongodb.org/manual/reference/aggregation/match/#pipe._S_match}
   *
   * @param coll
   * @param object
   * @return
   */
  private DBCollection match(DBCollection coll, DBObject object) {
    LOG.debug("computeResult() match : {}", object);

    List<DBObject> objects = coll.find((DBObject) object.get("$match")).toArray();
    coll = dropAndInsert(coll, objects);
    LOG.debug("computeResult() match : {}, result : {}", object, objects);
    return coll;
  }

  /**
   * Simple {@see http://docs.mongodb.org/manual/reference/aggregation/project/#pipe._S_project}
   * <p/>
   * TODO handle {bar : "$foo"}
   *
   * @param coll
   * @param object
   * @return
   */
  private DBCollection project(DBCollection coll, DBObject object) {
    LOG.debug("project() : {}", object);

    DBObject project = (DBObject) object.get("$project");
    DBObject projectResult = Util.clone(project);
    Map<String, String> renamedFields = new HashMap<String, String>();
    for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) project.toMap().entrySet()) {
      if (entry.getValue() != null && entry.getValue() instanceof String && entry.getValue().toString().startsWith("$")) {
        String realValue = entry.getValue().toString().substring(1);
        renamedFields.put(realValue, entry.getKey());
        projectResult.removeField(entry.getKey());

        // Handle complex case like $bar.foo
        if (realValue.contains(".")) {
          projectResult.put(realValue.substring(0, realValue.indexOf('.')), 1);
        } else {
          projectResult.put(realValue, 1);
        }
      }
    }

    LOG.debug("project() of {}", projectResult);
    List<DBObject> objects = coll.find(null, projectResult).toArray();

    // Rename fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = Util.clone(result);
      for (Map.Entry<String, String> entry : renamedFields.entrySet()) {
        if (Util.containsField(renamed, entry.getKey())) {
          Object value = Util.extractField(renamed, entry.getKey());
          renamed.put(entry.getValue(), value);
        }
      }

      // Two pass to remove the fields who are not wanted.
      // In first pass, we handle $bar.foo to renamed, but $bar still exist.
      // Now we remove it.
      for (Map.Entry<String, String> entry : renamedFields.entrySet()) {
        if (Util.containsField(renamed, entry.getKey())) {
          // Handle complex case like $bar.foo
          if (entry.getKey().contains(".")) {
            renamed.removeField(entry.getKey().substring(0, entry.getKey().indexOf('.')));
          } else {
            renamed.removeField(entry.getKey());
          }
        }
      }

      objectsResults.add(renamed);
    }
    coll = dropAndInsert(coll, objectsResults);
    LOG.debug("computeResult() project : {}, result : {}", object, objects);
    return coll;
  }

  /**
   * Drop collection and create new one with objects.
   *
   * @param coll
   * @param objects
   * @return the new collection.
   */
  private DBCollection dropAndInsert(DBCollection coll, List<DBObject> objects) {
    coll.drop();
    coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(objects);
    return coll;
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
}
