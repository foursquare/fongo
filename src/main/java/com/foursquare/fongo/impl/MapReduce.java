package com.foursquare.fongo.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class MapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class);

  private final FongoDB fongoDB;
  private final FongoDBCollection fongoDBCollection;
  private final String map;
  private final String reduce;
  private final DBObject out;
  private final DBObject query;

  public MapReduce(FongoDB fongoDB, FongoDBCollection coll, String map, String reduce, DBObject out, DBObject query) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.map = map;
    this.reduce = reduce;
    this.out = out;
    this.query = query;
  }

  /**
   * @return null if error.
   */
  public DBObject computeResult() {
    DBCollection coll = fongoDB.createCollection((String) out.get("replace"), null);
    // Mode replace.
    coll.remove(new BasicDBObject());

    Map<Object, List<Object>> mapKeyValue = new LinkedHashMap<Object, List<Object>>();
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("rhino");
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    StringBuilder construct = new StringBuilder();
    for (DBObject object : this.fongoDBCollection.find(query)) {
      String json = JSON.serialize(object);
      construct.setLength(0);
      construct.append("var $$$fongoOut1$$$ = null;\n");
      construct.append("var $$$fongoOut2$$$ = null;\n");
      construct.append("var obj = ").append(json).append(";\n");
      construct.append("function emit(param1, param2) { $$$fongoOut1$$$ = param1; $$$fongoOut2$$$ = param2; };\n");
      construct.append("obj[\"execute\"] = ").append(map).append("\n");
      construct.append("obj.execute();");

      try {
        bindings.clear();
        engine.eval(construct.toString());
        Object object1 = bindings.get("$$$fongoOut1$$$");
        Object object2 = bindings.get("$$$fongoOut2$$$");
        if (!mapKeyValue.containsKey(object1)) {            // TODO optim.
          mapKeyValue.put(object1, new ArrayList<Object>());
        }
        mapKeyValue.get(object1).add(object2);
        LOG.debug("emit({},{})", object1, object2);
      } catch (ScriptException e) {
        throw new RuntimeException(e); // TODO
      }
    }

    // Reduce.
    for (Map.Entry<Object, List<Object>> entry : mapKeyValue.entrySet()) {
      construct.setLength(0);
      construct.append("var reduce = ").append(reduce).append("\n");
      construct.append("var $$$fongoOut$$$ = \"\" + com.mongodb.util.JSON.serialize(reduce(").append(JSON.serialize(entry.getKey())).append(",").append(JSON.serialize(entry.getValue())).append(")).toString();");
      try {
        bindings.clear();
        engine.eval(construct.toString());
        String result = (String) engine.get("$$$fongoOut$$$");
        DBObject toInsert = new BasicDBObject(FongoDBCollection.ID_KEY, entry.getKey());
        toInsert.put("value", JSON.parse(String.valueOf(result)));
        coll.insert(toInsert);
      } catch (ScriptException e) {
        throw new RuntimeException(e); // TODO
      }
    }

    DBObject result = new BasicDBObject("collection", coll.getName()).append("db", coll.getDB().getName());
    LOG.debug("computeResult() : {}", result);
    return result;
  }
}
