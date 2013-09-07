package com.foursquare.fongo.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.List;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.Scriptable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final String finalize;
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

  public MapReduce(FongoDB fongoDB, FongoDBCollection coll, String map, String reduce, String finalize, DBObject out, DBObject query, DBObject sort, Number limit) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.map = map;
    this.reduce = reduce;
    this.finalize = finalize;
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
    Context cx = Context.enter();
    try {
      Scriptable scope = cx.initStandardObjects();
      List<DBObject> objects = this.fongoDBCollection.find(query).sort(sort).limit(limit).toArray();
      List<String> javascriptFunctions = constructJavascriptFunction(objects);
      for (String jsFunction : javascriptFunctions) {
        try {
          cx.evaluateString(scope, jsFunction, "<map-reduce>", 0, null);
        } catch (RhinoException e) {
          LOG.error("Exception running script {}", jsFunction, e);
          fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
        }
      }


      // Get the result into an object.
      NativeArray outs = (NativeArray) scope.get("$$$fongoOuts$$$", scope);
      for (int i = 0; i < outs.getLength(); i++) {
        NativeObject out = (NativeObject) outs.get(i, outs);
        outmode.newResult(coll, getObject(out));
      }

      DBObject result = new BasicDBObject("collection", coll.getName()).append("db", coll.getDB().getName());
      LOG.debug("computeResult() : {}", result);
      return result;
    } finally {
      cx.exit();
    }
  }

  DBObject getObject(NativeObject no) {
    DBObject ret = new BasicDBObject();
    Object[] propIds = no.getIds();
    for (Object propId : propIds) {
      String key = Context.toString(propId);
      Object value = NativeObject.getProperty(no, key);
      if (value instanceof NativeObject) {
        ret.put(key, getObject((NativeObject) value));
      } else {
        ret.put(key, value);
      }
    }
    return ret;
  }

  /**
   * Create the map/reduce/finalize function.
   */
  private List<String> constructJavascriptFunction(List<DBObject> objects) {
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
    sb.setLength(0);
    sb.append("var reduce = ").append(reduce).append("\n");
    sb.append("var $$$fongoOuts$$$ = Array();\n" +
        "for(i in $$$fongoEmits$$$) {\n" +
        "$$$fongoOuts$$$[$$$fongoOuts$$$.length] = { _id : i, value : reduce(i, $$$fongoEmits$$$[i]) };\n" +
        "}\n");
    result.add(sb.toString());

    return result;
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
