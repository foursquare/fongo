package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.util.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO : { project : { _id : 0} } must remove the _id field. If a $sort exist after...
 */
@ThreadSafe
public class Project extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Project.class);

  public static final Project INSTANCE = new Project();

  private Project() {
  }

  static abstract class ProjectedAbstract<T extends ProjectedAbstract> {
    protected static final Map<String, Class<? extends ProjectedAbstract>> projectedAbstractMap = new HashMap<String, Class<? extends ProjectedAbstract>>();

    static {
      projectedAbstractMap.put(ProjectedStrcasecmp.KEYWORD, ProjectedStrcasecmp.class);
      projectedAbstractMap.put(ProjectedCmp.KEYWORD, ProjectedCmp.class);
      projectedAbstractMap.put(ProjectedSubstr.KEYWORD, ProjectedSubstr.class);
      projectedAbstractMap.put(ProjectedIfNull.KEYWORD, ProjectedIfNull.class);
      projectedAbstractMap.put(ProjectedConcat.KEYWORD, ProjectedConcat.class);
      projectedAbstractMap.put(ProjectedToLower.KEYWORD, ProjectedToLower.class);
      projectedAbstractMap.put(ProjectedToUpper.KEYWORD, ProjectedToUpper.class);
    }

    final String keyword;

    final String destName;

    final List<String> infos = new ArrayList<String>();

    private ProjectedAbstract(String keyword, String destName, DBObject object) {
      this.keyword = keyword;
      this.destName = destName;
    }

    public T addInfo(String info) {
      infos.add(info);
      return (T) this;
    }

    /**
     * Transform the "object" into the "result" with this "value"
     *
     * @param result
     * @param object
     * @param key
     */
    public abstract void unapply(DBObject result, DBObject object, String key);

    abstract void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace);

    public final void apply(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, DBObject value, String namespace) {
      doWork(coll, projectResult, projectedFields, key, value.get(this.keyword), namespace);
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
    public static void createMapping(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object kvalue, String namespace, ProjectedAbstract projected) {
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
        ProjectedAbstract projectedAbstract = ProjectedAbstract.getProjected(value, coll, key);
        if (projectedAbstract != null) {
          // case : {cmp : {$cmp:[$firstname, $lastname]}}
          projectedAbstract.apply(coll, projectResult, projectedFields, key, value, namespace);
          projectResult.removeField(key);
        } else {
          // case : {biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" }}
          projectResult.removeField(key);
          for (Map.Entry<String, Object> subentry : (Set<Map.Entry<String, Object>>) value.toMap().entrySet()) {
            createMapping(coll, projectResult, projectedFields, subentry.getKey(), subentry.getValue(), namespace + key + ".", ProjectedRename.newInstance(namespace + key + "." + subentry.getKey(), coll, null));
          }
        }
      } else {
        // Case: {date : 1}
        projectedFields.put(key, projected.addInfo(key));
      }
    }

    /**
     * Search the projected field if any.
     *
     * @param value    the DbObject being worked.
     * @param coll     collection used.
     * @param destName destination name for the field.
     * @return null if it's not a keyword.
     */
    private static ProjectedAbstract getProjected(DBObject value, DBCollection coll, String destName) {
      for (Map.Entry<String, Class<? extends ProjectedAbstract>> entry : projectedAbstractMap.entrySet()) {
        if (value.containsField(entry.getKey())) {
          try {
            return entry.getValue().getConstructor(String.class, DBCollection.class, DBObject.class).newInstance(destName, coll, value);
          } catch (InstantiationException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof MongoException) {
              throw (MongoException) e.getTargetException();
            }
            throw new RuntimeException(e);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
          }
        }
      }

      return null;
    }

    protected static void errorResult(DBCollection coll, int code, String err) {
      ((FongoDB) coll.getDB()).notOkErrorResult(code, err).throwOnError();
    }

    /**
     * Extract a value from a field name or value.
     *
     * @param object
     * @param fieldOrValue
     * @param <T>
     * @return
     */
    protected static <T> T extractValue(DBObject object, Object fieldOrValue) {
      if (fieldOrValue instanceof String && fieldOrValue.toString().startsWith("$")) {
        return Util.extractField(object, fieldOrValue.toString().substring(1));
      }
      return (T) fieldOrValue;
    }
  }

  static class ProjectedRename extends ProjectedAbstract<ProjectedRename> {
    public static final String KEYWORD = "$___fongo$internal$";

    private ProjectedRename(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
    }

    public static ProjectedRename newInstance(String destName, DBCollection coll, DBObject object) {
      return new ProjectedRename(destName, coll, object);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      if (key != null) {
        Object value = Util.extractField(object, key);
        Util.putValue(result, destName, value);
      }
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
    }
  }

  static class ProjectedIfNull extends ProjectedAbstract<ProjectedIfNull> {
    public static final String KEYWORD = "$ifNull";

    private final String field;
    private final String valueIfNull;

    public ProjectedIfNull(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the $ifNull operator requires an array of 2 operands");
      }
      List<String> values = (List<String>) value;
      this.field = values.get(0);
      this.valueIfNull = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field, field, namespace, this);
      createMapping(coll, projectResult, projectedFields, valueIfNull, valueIfNull, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object value = extractValue(object, field);
      if (value == null) {
        value = extractValue(object, valueIfNull);
      }
      result.put(destName, value);
    }
  }

  static class ProjectedConcat extends ProjectedAbstract<ProjectedConcat> {
    public static final String KEYWORD = "$concat";

    private List<Object> toConcat = null;

    public ProjectedConcat(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() == 0) {
        errorResult(coll, 16020, "the $concat operator requires an array of operands");
      }
      toConcat = (List<Object>) value;
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
      for (Object field : toConcat) {
        if (field instanceof String) {
          createMapping(coll, projectResult, projectedFields, (String) field, (String) field, namespace, this);
        } else if (field instanceof DBObject) {
          // $concat : [ { $ifnull : [ "$item", "item is null" ] } ]
        }
      }
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      StringBuilder sb = new StringBuilder();
      for (Object info : toConcat) {
        Object value = extractValue(object, info);
        if (value == null) {
          result.put(destName, null);
          return;
        } else {
          String str = value.toString();
          sb.append(str);
        }
      }
      result.put(destName, sb.toString());
    }
  }

  static class ProjectedSubstr extends ProjectedAbstract<ProjectedSubstr> {
    public static final String KEYWORD = "$substr";

    private final String field;
    private final int start, end;

    public ProjectedSubstr(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 3) {
        errorResult(coll, 16020, "the $substr operator requires an array of 3 operands");
      }
      List<Object> values = (List<Object>) value;
      field = (String) values.get(0);
      start = ((Number) values.get(1)).intValue();
      end = ((Number) values.get(2)).intValue();
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, destName, destName, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object exracted = extractValue(object, field);
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

      result.put(destName, value);
    }
  }

  static class ProjectedCmp extends ProjectedAbstract<ProjectedCmp> {
    public static final String KEYWORD = "$cmp";

    private final String field1;
    private final String field2;

    public ProjectedCmp(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    public ProjectedCmp(String keyword, String destName, DBCollection coll, DBObject object) {
      super(keyword, destName, object);
      Object value = object.get(keyword);
      if (!(value instanceof List) || ((List) value).size() != 2) {
        errorResult(coll, 16020, "the " + keyword + "operator requires an array of 2 operands");
      }
      List<String> values = (List<String>) value;
      field1 = values.get(0);
      field2 = values.get(1);
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field1, field1, namespace, this);
      createMapping(coll, projectResult, projectedFields, field2, field2, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      String value = extractValue(object, field1).toString();
      String secondValue = extractValue(object, field2).toString();
      int strcmp = compare(value, secondValue);
      result.put(destName, strcmp < 0 ? -1 : strcmp > 1 ? 1 : 0);
    }

    protected int compare(String value1, String value2) {
      return value1.compareTo(value2);
    }
  }

  static class ProjectedStrcasecmp extends ProjectedCmp {
    public static final String KEYWORD = "$strcasecmp";

    public ProjectedStrcasecmp(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, coll, object);
    }

    @Override
    protected int compare(String value1, String value2) {
      return value1.compareToIgnoreCase(value2);
    }
  }

  static class ProjectedToLower extends ProjectedAbstract<ProjectedToLower> {
    public static final String KEYWORD = "$toLower";

    private final String field;

    public ProjectedToLower(String destName, DBCollection coll, DBObject object) {
      this(KEYWORD, destName, coll, object);
    }

    protected ProjectedToLower(String keyword, String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, object);
      Object value = object.get(keyword);
      if (value instanceof List) {
        List values = (List) value;
        if (values.size() != 1) {
          errorResult(coll, 16020, "the " + keyword + " operator requires 1 operand(s)");
        }
        field = (String) values.get(0);
      } else {
        field = value.toString();
      }
    }

    @Override
    void doWork(DBCollection coll, DBObject projectResult, Map<String, ProjectedAbstract> projectedFields, String key, Object value, String namespace) {
      createMapping(coll, projectResult, projectedFields, field, field, namespace, this);
    }

    @Override
    public void unapply(DBObject result, DBObject object, String key) {
      Object value = extractValue(object, field);
      if (value == null) {
        value = "";
      } else {
        value = transformValue(value.toString());
      }
      result.put(destName, value);
    }

    protected String transformValue(String value) {
      return value.toLowerCase();
    }
  }

  static class ProjectedToUpper extends ProjectedToLower {
    public static final String KEYWORD = "$toUpper";

    public ProjectedToUpper(String destName, DBCollection coll, DBObject object) {
      super(KEYWORD, destName, coll, object);
    }

    @Override
    protected String transformValue(String value) {
      return value.toUpperCase();
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
    Map<String, ProjectedAbstract> projectedFields = new HashMap<String, ProjectedAbstract>();
    for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) project.toMap().entrySet()) {
      if (entry.getValue() != null) {
        ProjectedAbstract.createMapping(coll, projectResult, projectedFields, entry.getKey(), entry.getValue(), "", ProjectedRename.newInstance(entry.getKey(), coll, null));
      }
    }

    LOG.debug("project() of {} renamed {}", projectResult, projectedFields);
    List<DBObject> objects = coll.find(null, projectResult).toArray();

    // Rename or transform fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY));
      for (Map.Entry<String, ProjectedAbstract> entry : projectedFields.entrySet()) {
        if (Util.containsField(result, entry.getKey())) {
          entry.getValue().unapply(renamed, result, entry.getKey());
        }
      }

      // TODO REFACTOR
      // Handle special case like ifNull who can doesn't have field in list.
      for (ProjectedAbstract projected : projectedFields.values()) {
//        if (!projected.isDone() && (projected.keyword.recallIfNotFound)) {
        projected.unapply(renamed, result, null);
//        }
//        projected.setDone(false);
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
