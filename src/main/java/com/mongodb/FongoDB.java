package com.mongodb;

import java.util.HashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
    super(fongo.getMongoClient(), name);
    this.fongo = fongo;
  }

  @Override
  public void requestStart() {}

  @Override
  public void requestDone() {}

  @Override
  public void requestEnsureConnection() {}

  @Override
  protected FongoDBCollection doGetCollection(String name) {
    synchronized(collMap){
      FongoDBCollection coll = collMap.get(name);
      if (coll == null){
        coll = new FongoDBCollection(this, name);
        collMap.put(name, coll);
      }
      return coll;
    }
  }

  @Override
  public Set<String> getCollectionNames() throws MongoException {
    return new HashSet<String>(collMap.keySet());
  }

  @Override
  public void cleanCursors(boolean force) throws MongoException {}

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
   * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
   * @param cmd dbobject representing the command to execute
   * @param options query options to use
   * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
   * @return result of command from the database
   * @dochub commands
   * @throws MongoException
   */
  @Override
  public CommandResult command( DBObject cmd , int options, ReadPreference readPrefs ) throws MongoException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fongo got command " + cmd);
    }
    if (cmd.containsField("getlasterror")) {
      return okResult();
    } else if (cmd.containsField("drop")) {
      this.collMap.remove(cmd.get("drop").toString());
      return okResult();
    } else if(cmd.containsField("create")) {
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
      okResult.append("n", result);
      return okResult;
    } else if(cmd.containsField("deleteIndexes")) {
      String collectionName = (String) cmd.get("deleteIndexes");
      String indexName = (String) cmd.get("index");
      if("*".equals(indexName)) {
        doGetCollection(collectionName)._dropIndexes();
      } else {
        doGetCollection(collectionName)._dropIndexes(indexName);
      }
      CommandResult okResult = okResult();
      return okResult;
    }
    return errorResult("undefined command: " + cmd);
  }

  public CommandResult okResult() {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", true);
    return result;
  }

  public CommandResult errorResult(String err) {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", false);
    result.put("err", err);
    return result;
  }

  public CommandResult errorResult(int code, String err) {
    CommandResult result = errorResult(err);
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
}
