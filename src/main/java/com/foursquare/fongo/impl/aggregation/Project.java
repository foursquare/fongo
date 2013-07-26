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
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Project extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Project.class);

  public static final Project INSTANCE = new Project();

  private Project() {
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
    Map<String, String> renamedFields = new HashMap<String, String>();
    for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) project.toMap().entrySet()) {
      if (entry.getValue() != null) {
        createMapping(projectResult, renamedFields, entry, "");
      }
    }

    LOG.info("project() of {} renamed {}", projectResult, renamedFields); // TODO
    List<DBObject> objects = coll.find(null, projectResult).toArray();

    // Rename fields
    List<DBObject> objectsResults = new ArrayList<DBObject>(objects.size());
    for (DBObject result : objects) {
      DBObject renamed = new BasicDBObject();
      for (Map.Entry<String, String> entry : renamedFields.entrySet()) {
        if (Util.containsField(result, entry.getKey())) {
          Object value = Util.extractField(result, entry.getKey());
          Util.putValue(renamed, entry.getValue(), value);
        }
      }

      objectsResults.add(renamed);
    }
    coll = dropAndInsert(coll, objectsResults);
    LOG.info("project() : {}, result : {}", object, objects);
    return coll;
  }

  /**
   * Create the mapping and the criteria for the collection.
   *
   * @param projectResult find criteria.
   * @param renamedFields mapping from criteria to project structure.
   * @param entry         from a DBObject.
   * @param namespace     "" if empty, "fieldname." elsewhere.
   */
  private void createMapping(DBObject projectResult, Map<String, String> renamedFields, Map.Entry<String, Object> entry, String namespace) {
    // Simple case : nb : "$pop"
    if (entry.getValue() instanceof String) {
      String value = entry.getValue().toString();
      if (value.startsWith("$")) {

        // Extract filename from projection.
        String fieldName = entry.getValue().toString().substring(1);
        // Prepare for renaming.
        renamedFields.put(fieldName, namespace + entry.getKey());
        projectResult.removeField(entry.getKey());

        // Handle complex case like $bar.foo with a little trick.
        if (fieldName.contains(".")) {
          projectResult.put(fieldName.substring(0, fieldName.indexOf('.')), 1);
        } else {
          projectResult.put(fieldName, 1);
        }
      } else {
        renamedFields.put(value, value);
      }
    } else if (entry.getValue() instanceof DBObject) {
      // biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" }
      DBObject value = (DBObject) entry.getValue();
      projectResult.removeField(entry.getKey());
      for (Map.Entry<String, Object> subentry : (Set<Map.Entry<String, Object>>) value.toMap().entrySet()) {
        createMapping(projectResult, renamedFields, subentry, namespace + entry.getKey() + ".");
      }
    }
  }

  @Override
  public String getKeyword() {
    return "$project";
  }

}
