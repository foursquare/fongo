package com.foursquare.fongo;

import com.mongodb.DB;
import com.mongodb.FongoDB;
import com.mongodb.MockMongoClient;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Faked out version of com.mongodb.Mongo
 * <p>
 * This class doesn't implement Mongo, but does provide the same basic interface
 * </p>
 * Usage:
 * <pre>
 * {@code
 * Fongo fongo = new Fongo("test server");
 * com.mongodb.DB db = fongo.getDB("mydb");
 * // if you need an instance of com.mongodb.Mongo
 * com.mongodb.MongoClient mongo = fongo.getMongo();
 * }
 * </pre>  
 * @author jon
 *
 */
public class Fongo {

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ServerAddress serverAddress;
  private final MongoClient mongo;
  private final String name;

  /**
   * 
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(String name) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultPort()));
    this.mongo = createMongo();
  }
  
  /**
   * equivalent to getDB in driver
   * multiple calls to this method return the same DB instance
   * @param dbname
   */
  public DB getDB(String dbname) {
    synchronized(dbMap) {      
      FongoDB fongoDb = dbMap.get(dbname);
      if (fongoDb == null) {
        fongoDb = new FongoDB(this, dbname);
        fongoDb.setWriteConcern(getWriteConcern());
        dbMap.put(dbname, fongoDb);
      }
      return fongoDb;
    }
  }

  /**
   * Get databases that have been used
   */
  public Collection<DB> getUsedDatabases() {
    return new ArrayList<DB>(dbMap.values());
  }

  /**
   * Get database names that have been used
   */
  public List<String> getDatabaseNames() {
    return new ArrayList<String>(dbMap.keySet());
  }

  /**
   * Drop db and all data from memory
   * @param dbName
   */
  public void dropDatabase(String dbName) {
    FongoDB db = dbMap.remove(dbName);
    if (db != null) {
      db.dropDatabase();
    }
  }
  
  /**
   * This will always be localhost:27017
   */
  public ServerAddress getServerAddress() {
    return serverAddress;
  }
  
  /**
   * A mocked out instance of com.mongodb.Mongo
   * All methods calls are intercepted and execute associated Fongo method 
   */
  public MongoClient getMongo() {
    return this.mongo;
  }
  
  public WriteConcern getWriteConcern() {
    return mongo.getWriteConcern();
  }
  
  private MongoClient createMongo() {
    return MockMongoClient.create(this);
  }
  
  @Override
  public String toString() {
    return "Fongo (" + this.name + ")";
  }

}
