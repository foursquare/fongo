package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.util.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO : $substr
 */
@ThreadSafe
public class Project extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Project.class);

  public static final Project INSTANCE = new Project();

  private Project() {
  }

  private enum Keyword {
    ALL(null) {

    },
    IFNULL("$ifNull"),
    CONCAT("$concat") {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, String> projectedFields, String key, Object value, String namespace) {
      }
    },
    SUBSTR("$substr"),
    STRCASECMP("$strcasecmp") {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, String> projectedFields, String key, Object value, String namespace) {
//	"errmsg" : "exception: the $strcasecmp operator requires an array of 2 operands",
//        "code" : 16019,
        if (!(value instanceof List) || ((List) value).size() != 2) {
          ((FongoDB) coll.getDB()).errorResult(16019, "the $strcasecmp operator requires an array of 2 operands").throwOnError();
        }
        List values = (List) value;
        createMapping(coll, projectResult, projectedFields, key, values.get(0), namespace);
        createMapping(coll, projectResult, projectedFields, key, values.get(1), namespace);
      }
    },
    CMP("$cmp"), // case sensitive
    TOLOWER("$toLower"),
    TOUPPER("$toUpper");

    private final String key;

    Keyword(String key) {
      this.key = key;
    }

    //@Nullable
    public static Keyword getKeyword(DBObject object) {
      Keyword ret = null;
      for (Keyword keyword : values()) {
        if (keyword.key != null && object.containsField(keyword.key)) {
          ret = keyword;
          break;
        }
      }
      return ret;
    }

    void doWork(DBCollection coll, DBObject projectResult, Map<String, String> projectedFields, String key, Object value, String namespace) {

    }

    public final void apply(DBCollection coll, DBObject projectResult, Map<String, String> projectedFields, String key, Object value, String namespace) {
      doWork(coll, projectResult, projectedFields, key, value, namespace);
    }

    /**
     * Create the mapping and the criteria for the collection.
     *
     * @param projectResult   find criteria.
     * @param projectedFields mapping from criteria to project structure.
     * @param key             key from a DBObject.
     * @param kvalue          value for k from a DBObject.
     * @param namespace       "" if empty, "fieldname." elsewhere.
     */
    void createMapping(DBCollection coll, DBObject projectResult, Map<String, String> projectedFields, String key, Object kvalue, String namespace) {
      // Simple case : nb : "$pop"
      if (kvalue instanceof String) {
        String value = kvalue.toString();
        if (value.startsWith("$")) {
          // Case { date: "$date"}

          // Extract filename from projection.
          String fieldName = kvalue.toString().substring(1);
          // Prepare for renaming.
          projectedFields.put(fieldName, namespace + key);
          projectResult.removeField(key);

          // Handle complex case like $bar.foo with a little trick.
          if (fieldName.contains(".")) {
            projectResult.put(fieldName.substring(0, fieldName.indexOf('.')), 1);
          } else {
            projectResult.put(fieldName, 1);
          }
        } else {
          projectedFields.put(value, value);
        }
      } else if (kvalue instanceof DBObject) {
        DBObject value = (DBObject) kvalue;
        Keyword keyword = Keyword.getKeyword(value);
        if (keyword != null) {
          // case : {cmp : {$cmp:[$firstname, $lastname]}}
          projectResult.removeField(key);
          keyword.apply(coll, projectResult, projectedFields, key, kvalue, namespace);
        } else {
          // case : {biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" }}
          projectResult.removeField(key);
          for (Map.Entry<String, Object> subentry : (Set<Map.Entry<String, Object>>) value.toMap().entrySet()) {
            createMapping(coll, projectResult, projectedFields, subentry.getKey(), subentry.getValue(), namespace + key + ".");
          }
        }
      } else {
        // Case: {date : 1}
        projectedFields.put(key, key);
      }
    }

  }

  /**
   * Simple {@see http://docs.mongodb.org/manual/reference/aggregation/project/#pipe._S_project}
   *
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    LOG.debug("project() : {}", object);

    DBObject project = (DBObject) object.get(getKeyword());
    DBObject projectResult = Util.clone(project);

    // Extract fields who will be renamed.
    Map<String, String> projectedFields = new HashMap<String, String>();
    for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) project.toMap().entrySet()) {
      if (entry.getValue() != null) {
        Keyword.ALL.createMapping(coll, projectResult, projectedFields, entry.getKey(), entry.getValue(), "");
      }
    }

    LOG.info("project() of {} renamed {}", projectResult, projectedFields); // TODO
    LOG.debug("project() of {} renamed {}", projectResult, projectedFields); // TODO
    List<DBObject> objects = coll.find(null, projectResult).toArray();

    // Rename fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = new BasicDBObject();
      for (Map.Entry<String, String> entry : projectedFields.entrySet()) {
        if (Util.containsField(result, entry.getKey())) {
          Object value = Util.extractField(result, entry.getKey());
          Util.putValue(renamed, entry.getValue(), value);
        }
      }

      objectsResults.add(renamed);
    }
    coll = dropAndInsert(coll, objectsResults);
    LOG.debug("project() : {}, result : {}", object, objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$project";
  }

}
