package com.mongodb;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.foursquare.fongo.Fongo;

public class FongoDB extends DB {

  private final Map<String, FongoDBCollection> collMap = Collections.synchronizedMap(new HashMap<String, FongoDBCollection>());
  private final Fongo fongo;
  private final boolean isDebug;
  
  public FongoDB(Fongo fongo, String name) {
    super(fongo.getMongo(), name);
    this.fongo = fongo;
    isDebug = fongo.isDebug();
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
  
  @Override
  public WriteConcern getWriteConcern() {
    return WriteConcern.NONE;
  }
  
  @Override
  public ReadPreference getReadPreference() {
    return ReadPreference.PRIMARY;
  }
  
  @Override
  public void dropDatabase() throws MongoException {
    this.fongo.dropDatabase(this.getName());
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
    if (isDebug) {
      System.out.println("Fongo got command " + cmd);
    }
    if (cmd.containsField("getlasterror")) {
      return okResult();
    } else if (cmd.containsField("drop")) {
      this.collMap.remove(cmd.get("drop").toString());
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

  @Override
  public String toString() {
    return "FongoDB." + this.getName();
  }

  public boolean isDebug() {
    return isDebug;
  }
}
