package com.foursquare.fongo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;

import com.mongodb.DB;
import com.mongodb.FongoDB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class Fongo implements MongoConnection {

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ServerAddress serverAddress;
  private final Mongo mongo;
  private String name;
  
  public Fongo(String name) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultPort()));
    this.mongo = createMongo();
  }
  
  @Override
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

  @Override
  public Collection<DB> getUsedDatabases() {
    return new ArrayList<DB>(dbMap.values());
  }

  @Override
  public List<String> getDatabaseNames() throws MongoException {
    return new ArrayList<String>(dbMap.keySet());
  }

  @Override
  public void dropDatabase(String dbName) throws MongoException {
    dbMap.remove(dbName);
  }
  
  public ServerAddress getServerAddress() {
    return serverAddress;
  }
  
  public Mongo getMongo() {
    return this.mongo;
  }
  
  private Mongo createMongo() {
    Mongo mongo = Mockito.mock(Mongo.class);
    Mockito.when(mongo.toString()).thenReturn("Fongo (" + this.name + ")");
    Mockito.when(mongo.getMongoOptions()).thenReturn(new MongoOptions());

    return mongo;
  }

}
