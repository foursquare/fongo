package com.foursquare.fongo.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.org.mozilla.javascript.NativeObject;

/**
 * http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
 * <p/>
 * TODO : finalize.
 */
public class MapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class);

  private final FongoDB fongoDB;
  private final FongoDBCollection fongoDBCollection;
  private final String map;
  private final String reduce;
  private final DBObject out;
  private final DBObject query;
  private final DBObject sort;
  private final int limit;

  // http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
  private enum Outmode {
    REPLACE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResult(DBCollection coll, DBObject result) {
        coll.insert(result);
      }
    },
    MERGE {
      @Override
      public void newResult(DBCollection coll, DBObject result) {
        // Upsert == insert the result if not exist.
        coll.update(new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY)), result, true, false);
      }
    },
    REDUCE {
      @Override
      public void newResult(DBCollection coll, DBObject result) {
        throw new IllegalStateException(); // Todo : recall "reduce function on two values..."
      }
    };

    public static Outmode valueFor(DBObject object) {
      for (Outmode outmode : values()) {
        if (object.containsField(outmode.name().toLowerCase())) {
          return outmode;
        }
      }
      return null;
    }

    public String collectionName(DBObject object) {
      return (String) object.get(name().toLowerCase());
    }

    public void initCollection(DBCollection coll) {
      // Do nothing.
    }

    public abstract void newResult(DBCollection coll, DBObject result);
  }

  public MapReduce(FongoDB fongoDB, FongoDBCollection coll, String map, String reduce, DBObject out, DBObject query, DBObject sort, Number limit) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.map = map;
    this.reduce = reduce;
    this.out = out;
    this.query = query;
    this.sort = sort;
    this.limit = limit == null ? 0 : limit.intValue();
  }

  /**
   * @return null if error.
   */
  public DBObject computeResult() {
    // Replace, merge or reduce ?
    Outmode outmode = Outmode.valueFor(out);
    DBCollection coll = fongoDB.createCollection(outmode.collectionName(out), null);
    // Mode replace.
    outmode.initCollection(coll);

    // TODO use Compilable ? http://www.jmdoudoux.fr/java/dej/chap-scripting.htm
    ScriptEngineManager scriptManager = new ScriptEngineManager();
    ScriptEngine engine = scriptManager.getEngineByName("rhino");

    if (LOG.isDebugEnabled()) {
      LOG.debug("engineName:{}, engineVersion:{}, languageVersion:{}", engine.getFactory().getEngineName(), engine.getFactory().getEngineVersion(), engine.getFactory().getLanguageVersion());
    }

    List<DBObject> objects = this.fongoDBCollection.find(query).sort(sort).limit(limit).toArray();
    List<String> mapFunctions = constructMapFunction(objects);
    for (String mapFunction : mapFunctions) {
      try {
        engine.eval(mapFunction);
      } catch (ScriptException e) {
        LOG.error("Exception running script {}", mapFunction, e);
        fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
      }
    }

    // Get the result into an object.
    NativeObject object1 = (NativeObject) engine.get("$$$fongoEmits$$$");
    StringBuilder sb = new StringBuilder(80000);
    for (Map.Entry<Object, Object> entry : object1.entrySet()) {
      // entry.getValue() is an array. (NativeArray)
      constructReduceFunction(sb, entry.getKey(), (List<Object>) entry.getValue());
      try {
        engine.eval(sb.toString());
        String result = (String) engine.get("$$$fongoOut$$$");
        DBObject toInsert = new BasicDBObject(FongoDBCollection.ID_KEY, entry.getKey());
        toInsert.put("value", JSON.parse(String.valueOf(result)));
        outmode.newResult(coll, toInsert);
      } catch (ScriptException e) {
        fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
      }
    }

    DBObject result = new BasicDBObject("collection", coll.getName()).append("db", coll.getDB().getName());
    LOG.debug("computeResult() : {}", result);
    return result;
  }

  /**
   * Create the map function.
   */
  private List<String> constructMapFunction(List<DBObject> objects) {
    List<String> result = new ArrayList<String>();
    StringBuilder sb = new StringBuilder(80000);
    // Add some function to javascript engine.
    addMongoFunctions(sb);

    // Create variables for exporting.
    sb.append("var $$$fongoEmits$$$ = new Object();\n");
    sb.append("function emit(param1, param2) { if(typeof $$$fongoEmits$$$[param1] === 'undefined') { " +
        "$$$fongoEmits$$$[param1] = new Array();" +
        "}\n" +
        "$$$fongoEmits$$$[param1][$$$fongoEmits$$$[param1].length] = param2;\n" +
        "};\n");
    // Prepare map function.
    sb.append("var fongoMapFunction = ").append(map).append(";\n");
    sb.append("var $$$fongoVars$$$ = new Object();\n");
    // For each object, execute in javascript the function.
    for (DBObject object : objects) {
      String json = JSON.serialize(object);
      sb.append("$$$fongoVars$$$ = ").append(json).append(";\n");
      sb.append("$$$fongoVars$$$['fongoExecute'] = fongoMapFunction;\n");
      sb.append("$$$fongoVars$$$.fongoExecute();\n");
      if (sb.length() > 65535) { // Rhino limit :-(
        result.add(sb.toString());
        sb.setLength(0);
      }
    }
    result.add(sb.toString());

    // Add Reduce Function
//    sb.setLength(0);
//    sb.append("var reduce = ").append(reduce).append("\n");
//    sb.append("var $$$fongoOut$$$ = null;\n");
//    sb.append("var $$$fongoOut$$$ = \"\" + com.mongodb.util.JSON.serialize(reduce(").append(JSON.serialize(entry.getKey())).append(",").append(JSON.serialize(entry.getValue())).append(")).toString();");

    return result;
  }

  /**
   * Create the reduce function.
   */
  private void constructReduceFunction(StringBuilder construct, Object key, List<Object> values) {
    construct.setLength(0);

    construct.append("var reduce = ").append(reduce).append("\n");
    construct.append("var $$$fongoOut$$$ = \"\" + com.mongodb.util.JSON.serialize(reduce(").append(JSON.serialize(key)).append(",").append(JSON.serialize(values)).append(")).toString();");
  }

  private void addMongoFunctions(StringBuilder construct) {
    // Add some function to javascript engine.
    construct.append("Array.sum = function(array) {\n" +
        "    var a = 0;\n" +
        "    for (var i = 0; i < array.length; i++) {\n" +
        "        a = a + array[i];\n" +
        "    }\n" +
        "    return a;" +
        "};\n");
  }
}
