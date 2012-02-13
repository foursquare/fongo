package com.foursquare.fongo;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import com.mongodb.DB;
import com.mongodb.FongoDB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class Fongo implements MongoConnection {

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ObjectInstantiator instantiator = new ObjenesisStd().getInstantiatorOf(Mongo.class);
  private final ServerAddress serverAddress;
  private final Mongo mongo;
  
  public Fongo() {
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
    Mongo mongo = (Mongo) instantiator.newInstance();
    try {
      Field field = Mongo.class.getDeclaredField("_options");
      field.setAccessible(true);
      field.set(mongo, new MongoOptions());
   } catch (Exception e) {
     throw new RuntimeException(e);
   }
    return mongo;
  }

}
