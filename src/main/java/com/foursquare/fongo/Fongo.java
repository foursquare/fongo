package com.foursquare.fongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.mongodb.DB;
import com.mongodb.FongoDB;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

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
 * com.mongodb.Mongo mongo = fongo.getMongo();
 * }
 * </pre>  
 * @author jon
 *
 */
public class Fongo {

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ServerAddress serverAddress;
  private final Mongo mongo;
  private final MongoClient mongoClient;
  private final String name;
  private WriteConcern concern = WriteConcern.ACKNOWLEDGED;

  /**
   * 
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(String name) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultPort()));
    this.mongo = createMongo();
    this.mongoClient = createMongoClient();
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
  public Mongo getMongo() {
    return this.mongo;
  }

  /**
   * A mocked out instance of {@link com.mongodb.MongoClient}
   * All methods calls are intercepted and execute associated Fongo method
   */
  public MongoClient getMongoClient() {
    return this.mongoClient;
  }

  public WriteConcern getWriteConcern() {
    return concern ;
  }
  
  private Mongo createMongo() {
    Mongo mongo = Mockito.mock(Mongo.class);
    return setupMongo(mongo);
  }

  private MongoClient createMongoClient() {
    MongoClient mongoClient = Mockito.mock(MongoClient.class);
    Mockito.when(mongoClient.getMongoClientOptions()).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return MongoClientOptions.builder().writeConcern(concern).build();
      }
    });
    return setupMongo(mongoClient);
  }

  private <T extends Mongo> T setupMongo(T mongo) {
    Mockito.when(mongo.toString()).thenReturn(toString());
    Mockito.when(mongo.getMongoOptions()).thenReturn(new MongoOptions());
    Mockito.when(mongo.getDB(Mockito.anyString())).thenAnswer(new Answer<DB>(){
      @Override
      public DB answer(InvocationOnMock invocation) throws Throwable {
        String dbName = (String) invocation.getArguments()[0];
        return getDB(dbName);
      }});
    Mockito.when(mongo.getUsedDatabases()).thenAnswer(new Answer<Collection<DB>>(){
      @Override
      public Collection<DB> answer(InvocationOnMock invocation) throws Throwable {
        return getUsedDatabases();
      }});
    Mockito.when(mongo.getDatabaseNames()).thenAnswer(new Answer<List<String>>(){
      @Override
      public List<String> answer(InvocationOnMock invocation) throws Throwable {
        return getDatabaseNames();
      }});
    Mockito.doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        String dbName = (String) invocation.getArguments()[0];
        dropDatabase(dbName);
        return null;
      }}).when(mongo).dropDatabase(Mockito.anyString());
    Mockito.when(mongo.getWriteConcern()).thenReturn(getWriteConcern());
    Mockito.doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        concern = (WriteConcern) invocation.getArguments()[0];
        return null;
      }}).when(mongo).setWriteConcern(Mockito.any(WriteConcern.class));
    return mongo;
  }

  @Override
  public String toString() {
    return "Fongo (" + this.name + ")";
  }

}
