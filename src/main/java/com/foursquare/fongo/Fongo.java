package com.foursquare.fongo;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
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

  private Map<String, FongoDB> dbMap = new HashMap<String, FongoDB>();
  private final ObjectInstantiator instantiator = new ObjenesisStd().getInstantiatorOf(Mongo.class);
  private final ServerAddress serverAddress;
  
  public Fongo() {
    try {
      serverAddress = new ServerAddress("localhost");
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public DB getDB(String dbname) {
    FongoDB fongoDb = dbMap.get(dbname);
    if (fongoDb == null) {
      fongoDb = new FongoDB(this, dbname);
      dbMap.put(dbname, fongoDb);
    }

    return fongoDb;
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
  
  public Mongo createMongo() {
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
