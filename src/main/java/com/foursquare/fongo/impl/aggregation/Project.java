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

    /**
     * Set to true when work is done (unapply successful).
     */
    boolean done = false;

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

    public void done() {
      done = true;
    }

    public boolean isDone() {
      return done;
    }

    public void setDone(boolean done) {
      this.done = done;
    }
  }

  private enum Keyword {
    RENAME(null) {
      // Only a renaming.
      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        Object value = Util.extractField(object, key);
        Util.putValue(result, projected.destName, value);
        projected.done();
      }
    },
    ALL(null) {

    },
    IFNULL("$ifNull", true) {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {
        if (!(value instanceof List) || ((List) value).size() != 2) {
//	"errmsg" : "exception: the $strcasecmp operator requires an array of 2 operands",
//        "code" : 16019,
          errorResult(coll, 16020, "the $ifNull operator requires an array of 2 operands");
        }
        List<String> values = (List<String>) value;
        Projected projected = Projected.projection(this, key);
        createMapping(coll, projectResult, projectedFields, (String) values.get(0), values.get(0), namespace, projected);
        createMapping(coll, projectResult, projectedFields, (String) values.get(1), values.get(1), namespace, projected);
        projected.done();
      }

      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        Object value = extractValue(object, projected.infos.get(0));
        if (value == null) {
          value = extractValue(object, projected.infos.get(1));
        }
        result.put(projected.destName, value);
        projected.done();
      }

    },
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
          Object value = extractValue(object, info);
          if (value == null) {
            result.put(projected.destName, null);
            return;
          } else {
            String str = value.toString();
            sb.append(str);
          }
        }
        result.put(projected.destName, sb.toString());
        projected.done();
      }
    },
    SUBSTR("$substr", true) {
      @Override
      void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {
        if (!(value instanceof List) || ((List) value).size() != 3) {
          //com.mongodb.CommandFailureException: { "serverUsed" : "/127.0.0.1:27017" , "errmsg" : "exception: the $substr operator requires 3 operand(s)" , "code" : 16020 , "ok" : 0.0}
          errorResult(coll, 16020, "the $substr operator requires an array of 3 operands");
        }
        List<Object> values = (List<Object>) value;
        Projected projected = Projected.projection(this, key);
        projected.addInfo(String.valueOf(((Number) values.get(1)).intValue()));
        projected.addInfo(String.valueOf(((Number) values.get(2)).intValue()));
        createMapping(coll, projectResult, projectedFields, (String) values.get(0), values.get(0), namespace, projected);
      }

      @Override
      public void unapply(DBObject result, DBObject object, Projected projected, String key) {
        int start = Integer.valueOf(projected.infos.get(0));
        int end = Integer.valueOf(projected.infos.get(1));
        Object exracted = extractValue(object, projected.infos.get(2));
        String value = exracted == null ? null : String.valueOf(exracted);
        if (value == null) {
          value = "";
        } else {
          if (start >= value.length()) {
            value = "";
          } else {
            value = value.substring(start, Math.min(end, value.length()));
          }
        }

        result.put(projected.destName, value);
        projected.done();
      }

    },
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
        String value = extractValue(object, projected.infos.get(0)).toString().toLowerCase();
        String secondValue = extractValue(object, projected.infos.get(1)).toString().toLowerCase();
        int strcmp = value.compareTo(secondValue);
        result.put(projected.destName, strcmp < 0 ? -1 : strcmp > 1 ? 1 : 0);
        projected.done();
      }
    },
    CMP("$cmp"), // case sensitive
    TOLOWER("$toLower"),
    TOUPPER("$toUpper");

    final String keyword;

    /**
     * Set to true = call the "Projected" if we can't find values (like ifNull, substr, etc)
     */
    final boolean recallIfNotFound;

    Keyword(String key) {
      this(key, false);
    }

    Keyword(String key, boolean recallIfNotFound) {
      this.keyword = key;
      this.recallIfNotFound = recallIfNotFound;
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

    // TODO : must be abstract
    void doWork(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, Object value, String namespace) {

    }

    public final void apply(DBCollection coll, DBObject projectResult, Map<String, Projected> projectedFields, String key, DBObject value, String namespace) {
      doWork(coll, projectResult, projectedFields, key, value.get(this.keyword), namespace);
    }

    private static <T> T extractValue(DBObject object, Object fieldOrValue) {
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
            createMapping(coll, projectResult, projectedFields, subentry.getKey(), subentry.getValue(), namespace + key + ".", Projected.defaultProjection(namespace + key + "." + subentry.getKey()));
          }
        }
      } else {
        // Case: {date : 1}
        projectedFields.put(key, projected.addInfo(key));
      }
    }

    private static void errorResult(DBCollection coll, int code, String err) {
      ((FongoDB) coll.getDB()).notOkErrorResult(code, err).throwOnError();
    }

    // TODO : must be abstract
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

      // TODO REFACTOR
      // Handle special case like ifNull who can doesn't have field in list.
      for (Projected projected : projectedFields.values()) {
        if (!projected.isDone() && (projected.keyword.recallIfNotFound)) {
          projected.unapply(renamed, result, null);
        }
        projected.setDone(false);
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
