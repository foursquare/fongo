package com.mongodb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.foursquare.fongo.Fongo;

public class FongoDB extends DB {

  private final Map<String, FongoDBCollection> collMap = Collections.synchronizedMap(new HashMap<String, FongoDBCollection>());
  private final Fongo fongo;
  
  public FongoDB(Fongo fongo, String name) {
    super(fongo.getMongo(), name);
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
    return Collections.unmodifiableSet(collMap.keySet());
  }

  @Override
  public void cleanCursors(boolean force) throws MongoException {}
  
  @Override
  public DB getSisterDB(String name) {
   return fongo.getDB(name);
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
    System.out.println("Fongo got command " + cmd);
    if (cmd.containsField("count")) {
      String collectionName = cmd.get("count").toString();
      int count = doGetCollection(collectionName).fCount((DBObject)cmd.get("query"));
      CommandResult result = okResult();
      result.put("n", count);
      return result;
    } else if (cmd.containsField("findandmodify")) {
      String collectionName = cmd.get("findandmodify").toString();
      DBObject query = (DBObject)cmd.get("query");
      DBObject update = (DBObject)cmd.get("update");
      DBObject sort = (DBObject)cmd.get("sort");
      boolean remove = Boolean.valueOf(String.valueOf(cmd.get("remove")));
      boolean upsert = Boolean.valueOf(String.valueOf(cmd.get("upsert")));
      boolean returnNew = Boolean.valueOf(String.valueOf(cmd.get("new")));

      DBObject dbResult = doGetCollection(collectionName).fFindAndModify(query, update, sort, remove, returnNew, upsert);
      CommandResult commandResult = okResult();
      commandResult.put("value", dbResult);
      return commandResult;
    } else if (cmd.containsField("getlasterror")) {
      return okResult();
    }
    CommandResult errorResult = new CommandResult(fongo.getServerAddress());
    errorResult.put("err", "undefined command: " + cmd);
    return errorResult;
  }

  public CommandResult okResult() {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", true);
    return result;
  }

}
