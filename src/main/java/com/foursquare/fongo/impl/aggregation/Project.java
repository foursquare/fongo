package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
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

  private static class Projected {
    final Keyword keyword;

    final String destName;

    final List<String> infos = new ArrayList<String>();

    final List<Object> results = new ArrayList<Object>();

    private Projected(Keyword keyword, String destName) {
      this.keyword = keyword;
      this.destName = destName;
    }

    public Projected addInfo(String info) {
      infos.add(info);
      return this;
    }

    public Projected addResult(Object result) {
      results.add(result);
      return this;
    }

    public static Projected defaultProjection(String destName) {
      return projection(Keyword.RENAME, destName);
    }

    public static Projected projection(Keyword keyword, String destName) {
      return new Projected(keyword, destName);
    }

    /**
     * Transform the "object" into the "result" with this "value"
     *
     * @param result
     * @param object
     * @param key
     */
    public void unapply(DBObject result, DBObject object, String key) {
      this.keyword.unapply(result, object, this, key);
    }
  }

  private enum Keyword {
    RENAME(null) {
      // Only a renaming.
      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        Object value = Util.extractField(object, key);
        Util.putValue(result, projected.destName, value);
      }
    },
    ALL(null) {

    },
    IFNULL("$ifNull"),
    CONCAT("$concat") {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {
        if (!(value instanceof List) || ((List) value).size() == 0) {
//	"errmsg" : "exception: the $strcasecmp operator requires an array of 2 operands",
//        "code" : 16019,
          errorResult(coll, 16020, "the $concat operator requires an array of operands"); // TODO
        }
        List<Object> fields = (List<Object>) value;
        Projected projected = Projected.projection(this, key);
        for (Object field : fields) {
          if (field instanceof String) {
            createMapping(coll, projectResult, projectedFields, (String) field, (String) field, namespace, projected);
          } else if (field instanceof DBObject) {
            // $concat : [ { $ifnull : [ "$item", "item is null" ] } ]
          }
        }
      }

      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        StringBuilder sb = new StringBuilder();
        for (String info : projected.infos) {
          Object value = extracetValue(object, info);
          if (value == null) {
            result.put(projected.destName, null);
            return;
          } else {
            String str = value.toString();
            sb.append(str);
          }
        }
        result.put(projected.destName, sb.toString());
      }
    },
    SUBSTR("$substr"),
    STRCASECMP("$strcasecmp") {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {
        if (!(value instanceof List) || ((List) value).size() != 2) {
//	"errmsg" : "exception: the $strcasecmp operator requires an array of 2 operands",
//        "code" : 16019,
          errorResult(coll, 16020, "the $strcasecmp operator requires an array of 2 operands");
        }
        List<String> values = (List<String>) value;
        Projected projected = Projected.projection(this, key);
        createMapping(coll, projectResult, projectedFields, (String) values.get(0), values.get(0), namespace, projected);
        createMapping(coll, projectResult, projectedFields, (String) values.get(1), values.get(1), namespace, projected);
      }

      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        String value = extracetValue(object, projected.infos.get(0)).toString().toLowerCase();
        String secondValue = extracetValue(object, projected.infos.get(1)).toString().toLowerCase();
        int strcmp = value.compareTo(secondValue);
        result.put(projected.destName, strcmp < 0 ? -1 : strcmp > 1 ? 1 : 0);
      }
    },
    CMP("$cmp"), // case sensitive
    TOLOWER("$toLower"),
    TOUPPER("$toUpper");

    final String keyword;

    Keyword(String key) {
      this.keyword = key;
    }

    //@Nullable
    public static Keyword getKeyword(DBObject object) {
      Keyword ret = null;
      for (Keyword keyword : values()) {
        if (keyword.keyword != null && object.containsField(keyword.keyword)) {
          ret = keyword;
          break;
        }
      }
      return ret;
    }

    void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {

    }

    public final void apply(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, DBObject value, String namespace) {
      doWork(coll, projectResult, projectedFields, key, value.get(this.keyword), namespace);
    }

    private static <T> T extracetValue(DBObject object, Object fieldOrValue) {
      if (fieldOrValue instanceof String && fieldOrValue.toString().startsWith("$")) {
        return Util.extractField(object, fieldOrValue.toString().substring(1));
      }
      return (T) fieldOrValue;
    }

    /**
     * Create the mapping and the criteria for the collection.
     *
     * @param projectResult   find criteria.
     * @param projectedFields mapping from criteria to project structure.
     * @param key             keyword from a DBObject.
     * @param kvalue          value for k from a DBObject.
     * @param namespace       "" if empty, "fieldname." elsewhere.
     * @param projected       use for unapplying.
     */
    void createMapping(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object kvalue, String namespace, Projected projected) {
      // Simple case : nb : "$pop"
      if (kvalue instanceof String) {
        String value = kvalue.toString();
        if (value.startsWith("$")) {
          // Case { date: "$date"}

          // Extract filename from projection.
          String fieldName = kvalue.toString().substring(1);
          // Prepare for renaming.
          projectedFields.put(fieldName, projected.addInfo(namespace + key));
          projectResult.removeField(key);

          // Handle complex case like $bar.foo with a little trick.
          if (fieldName.contains(".")) {
            projectResult.put(fieldName.substring(0, fieldName.indexOf('.')), 1);
          } else {
            projectResult.put(fieldName, 1);
          }
        } else {
          projectedFields.put(value, projected.addInfo(value));
        }
      } else if (kvalue instanceof DBObject) {
        DBObject value = (DBObject) kvalue;
        Keyword keyword = Keyword.getKeyword(value);
        if (keyword != null) {
          // case : {cmp : {$cmp:[$firstname, $lastname]}}
          keyword.apply(coll, projectResult, projectedFields, key, value, namespace);
          projectResult.removeField(key);
        } else {
          // case : {biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" }}
          projectResult.removeField(key);
          for (Map.Entry<String, Object> subentry : (Set<Map.Entry<String, Object>>) value.toMap().entrySet()) {
            createMapping(coll, projectResult, projectedFields, subentry.getKey(), subentry.getValue(), namespace + key + ".", Projected.defaultProjection(subentry.getKey()));
          }
        }
      } else {
        // Case: {date : 1}
        projectedFields.put(key, projected.addInfo(key));
      }
    }

    private static void errorResult(DBCollection coll, int code, String err) {
      ((FongoDB) coll.getDB()).errorResult(code, err).throwOnError();
    }

    // Translate from result of find to user field.
    public void unapply(DBObject result, DBObject object, Projected projected, String key) {
      Object value = Util.extractField(object, key);
      Util.putValue(result, projected.infos.get(0), value);
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
    Map<String, Projected> projectedFields = new HashMap<String, Projected>();
    for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) project.toMap().entrySet()) {
      if (entry.getValue() != null) {
        Keyword.ALL.createMapping(coll, projectResult, projectedFields, entry.getKey(), entry.getValue(), "", Projected.defaultProjection(entry.getKey()));
      }
    }

    LOG.info("project() of {} renamed {}", projectResult, projectedFields); // TODO
    LOG.debug("project() of {} renamed {}", projectResult, projectedFields); // TODO
    List<DBObject> objects = coll.find(null, projectResult).toArray();
    LOG.info("project() of {} result {}", projectResult, objects); // TODO

    // Rename or transform fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY));
      for (Map.Entry<String, Projected> entry : projectedFields.entrySet()) {
        if (Util.containsField(result, entry.getKey())) {
          entry.getValue().unapply(renamed, result, entry.getKey());
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
