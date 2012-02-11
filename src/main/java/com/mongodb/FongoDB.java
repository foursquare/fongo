package com.mongodb;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.foursquare.fongo.Fongo;

public class FongoDB extends DB {

  private final ConcurrentMap<String, FongoDBCollection> collMap = new ConcurrentHashMap<String, FongoDBCollection>();
  private final Fongo fongo;
  
  public FongoDB(Fongo fongo, String name) {
    super(fongo.createMongo(), name);
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
    FongoDBCollection newColl = new FongoDBCollection(this, name);
    FongoDBCollection oldColl = collMap.putIfAbsent(name, newColl);
    return oldColl == null ? newColl : oldColl;
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
    System.out.println("Got command " + cmd);
    if (cmd.containsField("count")) {
      String collectionName = cmd.get("count").toString();
      int count = doGetCollection(collectionName).fCount((DBObject)cmd.get("query"));
      CommandResult result = new CommandResult(fongo.getServerAddress());
      result.put("ok", true);
      result.put("n", count);
      return result;
    }
    CommandResult errorResult = new CommandResult(fongo.getServerAddress());
    errorResult.put("err", "undefined command: " + cmd);
    return errorResult;
  }

}
