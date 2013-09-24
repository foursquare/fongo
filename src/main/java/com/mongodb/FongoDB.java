package com.mongodb;

import com.foursquare.fongo.impl.Aggregator;
import com.foursquare.fongo.impl.MapReduce;
import java.util.HashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foursquare.fongo.Fongo;

/**
 * fongo override of com.mongodb.DB
 * you shouldn't need to use this class directly
 *
 * @author jon
 */
public class FongoDB extends DB {
  final static Logger LOG = LoggerFactory.getLogger(FongoDB.class);

  private final Map<String, FongoDBCollection> collMap = Collections.synchronizedMap(new HashMap<String, FongoDBCollection>());
  private final Fongo fongo;

  public FongoDB(Fongo fongo, String name) {
    super(fongo.getMongo(), name);
    this.fongo = fongo;
    doGetCollection("system.users");
    doGetCollection("system.indexes");
  }

  @Override
  public void requestStart() {
  }

  @Override
  public void requestDone() {
  }

  @Override
  public void requestEnsureConnection() {
  }

  @Override
  protected FongoDBCollection doGetCollection(String name) {
    synchronized (collMap) {
      FongoDBCollection coll = collMap.get(name);
      if (coll == null) {
        coll = new FongoDBCollection(this, name);
        collMap.put(name, coll);
      }
      return coll;
    }
  }

  private List<DBObject> doAggregateCollection(String collection, List<DBObject> pipeline) {
    FongoDBCollection coll = doGetCollection(collection);
    Aggregator aggregator = new Aggregator(this, coll, pipeline);

    return aggregator.computeResult();
  }

  private DBObject doMapReduce(String collection, String map, String reduce, DBObject out, DBObject query, DBObject sort, Number limit) {
    FongoDBCollection coll = doGetCollection(collection);
    MapReduce mapReduce = new MapReduce(this, coll, map, reduce, out, query, sort, limit);
    return mapReduce.computeResult();
  }

  private List<DBObject> doGeoNearCollection(String collection, DBObject near, DBObject query, Number limit, Number maxDistance, boolean spherical) {
    FongoDBCollection coll = doGetCollection(collection);
    return coll.geoNear(near, query, limit, maxDistance, spherical);
  }


  @Override
  public Set<String> getCollectionNames() throws MongoException {
    return new HashSet<String>(collMap.keySet());
  }

  @Override
  public void cleanCursors(boolean force) throws MongoException {
  }

  @Override
  public DB getSisterDB(String name) {
    return fongo.getDB(name);
  }

  @Override
  public WriteConcern getWriteConcern() {
    return fongo.getWriteConcern();
  }

  @Override
  public ReadPreference getReadPreference() {
    return ReadPreference.primaryPreferred();
  }

  @Override
  public void dropDatabase() throws MongoException {
    this.fongo.dropDatabase(this.getName());
    for (FongoDBCollection c : new ArrayList<FongoDBCollection>(collMap.values())) {
      c.drop();
    }
  }

  @Override
  CommandResult doAuthenticate(MongoCredential credentials) {
    return okResult();
  }

  /**
   * Executes a database command.
   *
   * @param cmd       dbobject representing the command to execute
   * @param options   query options to use
   * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
   * @return result of command from the database
   * @throws MongoException
   * @dochub commands
   * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
   */
  @Override
  public CommandResult command(DBObject cmd, int options, ReadPreference readPrefs) throws MongoException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fongo got command " + cmd);
    }
    if (cmd.containsField("getlasterror")) {
      return okResult();
    } else if (cmd.containsField("drop")) {
      this.collMap.remove(cmd.get("drop").toString());
      return okResult();
    } else if (cmd.containsField("create")) {
      String collectionName = (String) cmd.get("create");
      doGetCollection(collectionName);
      return okResult();
    } else if (cmd.containsField("count")) {
      String collectionName = (String) cmd.get("count");
      Number limit = (Number) cmd.get("limit");
      Number skip = (Number) cmd.get("skip");
      long result = doGetCollection(collectionName).getCount(
          (DBObject) cmd.get("query"),
          null,
          limit == null ? 0L : limit.longValue(),
          skip == null ? 0L : skip.longValue());
      CommandResult okResult = okResult();
      okResult.append("n", (double) result);
      return okResult;
    } else if (cmd.containsField("deleteIndexes")) {
      String collectionName = (String) cmd.get("deleteIndexes");
      String indexName = (String) cmd.get("index");
      if ("*".equals(indexName)) {
        doGetCollection(collectionName)._dropIndexes();
      } else {
        doGetCollection(collectionName)._dropIndexes(indexName);
      }
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("aggregate")) {
      List<DBObject> result = doAggregateCollection((String) cmd.get("aggregate"), (List<DBObject>) cmd.get("pipeline"));
      if (result == null) {
        return notOkErrorResult("can't aggregate");
      }
      CommandResult okResult = okResult();
      BasicDBList list = new BasicDBList();
      list.addAll(result);
      okResult.put("result", list);
      return okResult;
    } else if (cmd.containsField("ping")) {
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("validate")) {
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("buildInfo")) {
      CommandResult okResult = okResult();
      okResult.put("version", "2.4.5");
      okResult.put("maxBsonObjectSize", 16777216);
      return okResult;
    } else if(cmd.containsField("forceerror")) {
      // http://docs.mongodb.org/manual/reference/command/forceerror/
      CommandResult result = notOkErrorResult(10038, null, "exception: forced error");
      return result;
    } else if (cmd.containsField("mapreduce")) {
      // TODO : sort/limit
      DBObject result = doMapReduce((String) cmd.get("mapreduce"), (String) cmd.get("map"), (String) cmd.get("reduce"), (DBObject) cmd.get("out"), (DBObject) cmd.get("query"), (DBObject) cmd.get("sort"), (Number) cmd.get("limit"));
      if (result == null) {
        return notOkErrorResult("can't mapReduce");
      }
      CommandResult okResult = okResult();
      if(result instanceof List) {
        // INLINE case.
        okResult.put("results", result);
      } else {
        okResult.put("result", result);
      }
      return okResult;
    } else if (cmd.containsField("geoNear")) {
      // http://docs.mongodb.org/manual/reference/command/geoNear/
      // TODO : handle "num" (override limit)
      try {
        List<DBObject> result = doGeoNearCollection((String) cmd.get("geoNear"),
            (DBObject) cmd.get("near"),
            (DBObject) cmd.get("query"),
            (Number) cmd.get("limit"),
            (Number) cmd.get("maxDistance"),
            Boolean.TRUE.equals(cmd.get("spherical")));
        if (result == null) {
          return notOkErrorResult("can't geoNear");
        }
        CommandResult okResult = okResult();
        BasicDBList list = new BasicDBList();
        list.addAll(result);
        okResult.put("results", list);
        return okResult;
      } catch (MongoException me) {
        CommandResult result = errorResult(me.getCode(), me.getMessage());
        return result;
      }
    }
    String command = cmd.toString();
    if (!cmd.keySet().isEmpty()) {
      command = cmd.keySet().iterator().next();
    }
    return notOkErrorResult(null, "no such cmd: " + command);
  }

  public CommandResult okResult() {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", 1.0);
    return result;
  }

  public CommandResult notOkErrorResult(String err) {
    return notOkErrorResult(err, null);
  }

  public CommandResult notOkErrorResult(String err, String errmsg) {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", 0.0);
    if (err != null) {
      result.put("err", err);
    }
    if (errmsg != null) {
      result.put("errmsg", errmsg);
    }
    return result;
  }

  public CommandResult notOkErrorResult(int code, String err) {
    CommandResult result = notOkErrorResult(err);
    result.put("code", code);
    return result;
  }

  public CommandResult notOkErrorResult(int code, String err, String errmsg) {
    CommandResult result = notOkErrorResult(err, errmsg);
    result.put("code", code);
    return result;
  }

  public CommandResult errorResult(int code, String err) {
    CommandResult result = okResult();
    result.put("err", err);
    result.put("code", code);
    return result;
  }

  @Override
  public String toString() {
    return "FongoDB." + this.getName();
  }

  public void removeCollection(FongoDBCollection collection) {
    collMap.remove(collection.getName());
  }

  public void addCollection(FongoDBCollection collection) {
    collMap.put(collection.getName(), collection);
  }
}
