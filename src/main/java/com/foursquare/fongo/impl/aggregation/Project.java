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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 24/07/13
 */
public class Project extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Project.class);

  public Project(FongoDB fongoDB) {
    super(fongoDB);
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
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    LOG.debug("project() : {}", object);

    DBObject project = (DBObject) object.get(getKeyword());
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
    LOG.debug("project() : {}, result : {}", object, objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$project";
  }

}
