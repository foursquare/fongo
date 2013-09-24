package com.foursquare.fongo.impl;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.util.JSON;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

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
        coll.update(new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY)), result, true,
            false);
      }
    },
    REDUCE {
      @Override
      public void newResult(DBCollection coll, DBObject result) {
        throw new IllegalStateException(); // Todo : recall "reduce function on two values..."
      }
    },
    INLINE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResult(DBCollection coll, DBObject result) {
        coll.insert(result);
      }

      @Override
      public String collectionName(DBObject object) {
        // Random uuid for extract result after.
        return UUID.randomUUID().toString();
      }

      // Return a list of all results.
      @Override
      public DBObject createResult(DBCollection coll) {
        BasicDBList list = new BasicDBList();
        list.addAll(coll.find().toArray());
        return list;
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

    public DBObject createResult(DBCollection coll) {
      DBObject result = new BasicDBObject("collection", coll.getName()).append("db", coll.getDB().getName());
      return result;
    }
  }

  public MapReduce(FongoDB fongoDB, FongoDBCollection coll, String map, String reduce, DBObject out, DBObject query,
                   DBObject sort, Number limit) {
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
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("rhino");
    if (LOG.isDebugEnabled()) {
      LOG.debug("engineName:{}, engineVersion:{}, languageVersion:{}", engine.getFactory().getEngineName(),
          engine.getFactory().getEngineVersion(), engine.getFactory().getLanguageVersion());
    }
    StringBuilder construct = new StringBuilder();
    Map<Object, List<Object>> mapKeyValue = new LinkedHashMap<Object, List<Object>>();
    for (DBObject object : this.fongoDBCollection.find(query).sort(sort).limit(limit)) {
      String json = JSON.serialize(object);
      constructMapFunction(construct, json);

      try {
        engine.eval(construct.toString());
        Object object1 = engine.get("$$$fongoOut1$$$");
        if (object1 != null) {
          Object object2 = engine.get("$$$fongoOut2$$$");
          List<Object> emitList = mapKeyValue.get(object1);
          if (emitList == null) {
            emitList = new ArrayList<Object>();
            mapKeyValue.put(object1, emitList);
          }
          emitList.add(object2);
          LOG.debug("emit({},{})", object1, object2);
        }
      } catch (ScriptException e) {
        fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
      }
    }

    // Reduce.
    for (Map.Entry<Object, List<Object>> entry : mapKeyValue.entrySet()) {
      constructReduceFunction(construct, entry);
      try {
        engine.eval(construct.toString());
        String result = (String) engine.get("$$$fongoOut$$$");
        DBObject toInsert = new BasicDBObject(FongoDBCollection.ID_KEY, entry.getKey());
        toInsert.put("value", JSON.parse(String.valueOf(result)));
        outmode.newResult(coll, toInsert);
      } catch (ScriptException e) {
        fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
      }
    }

    DBObject result = outmode.createResult(coll);
    LOG.debug("computeResult() : {}", result);
    return result;
  }

  /**
   * Create the map function.
   */
  private void constructMapFunction(StringBuilder construct, String json) {
    construct.setLength(0);
    // Add some function to javascript engine.
    addMongoFunctions(construct);
    construct.append("var $$$fongoOut1$$$ = null;\n");
    construct.append("var $$$fongoOut2$$$ = null;\n");
    construct.append("var obj = ").append(json).append(";\n");
    construct.append("function emit(param1, param2) { $$$fongoOut1$$$ = param1; $$$fongoOut2$$$ = param2; };\n");
    construct.append("obj[\"execute\"] = ").append(map).append("\n");
    construct.append("obj.execute();");
  }

  /**
   * Create the reduce function.
   */
  private void constructReduceFunction(StringBuilder construct, Map.Entry<Object, List<Object>> entry) {
    construct.setLength(0);
    addMongoFunctions(construct);

    construct.append("var reduce = ").append(reduce).append("\n");
    construct.append("var $$$fongoOut$$$ = \"\" + com.mongodb.util.JSON.serialize(reduce(")
        .append(JSON.serialize(entry.getKey()))
        .append(",")
        .append(JSON.serialize(entry.getValue()))
        .append(")).toString();");
  }

  private void addMongoFunctions(StringBuilder construct) {
    // Add some function to javascript engine.
    construct.append("Array.sum = function(array) {\n" + "    var a = 0;\n"
        + "    for (var i = 0; i < array.length; i++) {\n" + "        a = a + array[i];\n" + "    }\n" + "    return a;"
        + "};");
  }
}
