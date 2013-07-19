package com.mongodb;

import com.foursquare.fongo.Fongo;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import org.objenesis.ObjenesisStd;

public class MockMongoClient extends MongoClient {
  private Fongo fongo;
  private final MongoOptions options = new MongoOptions();

  public static MockMongoClient create(Fongo fongo) {
    MockMongoClient client = (MockMongoClient) new ObjenesisStd().getInstantiatorOf(MockMongoClient.class).newInstance();
    client.setFongo(fongo);
    return client;
  }
  
  public MockMongoClient() throws UnknownHostException {

  }
  
  public void setFongo(Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public String toString() {
    return fongo.toString();
  }

  @Override
  public Collection<DB> getUsedDatabases() {
    return fongo.getUsedDatabases();
  }

  @Override
  public List<String> getDatabaseNames() {
    return fongo.getDatabaseNames();
  }

  @Override
  public int getMaxBsonObjectSize() {
    return Bytes.MAX_OBJECT_SIZE;
  }

  @Override
  public DB getDB(String dbname) {
    return fongo.getDB(dbname);
  }

  @Override
  public void dropDatabase(String dbName) {
    fongo.dropDatabase(dbName);
  }
  
  @Override
  boolean isMongosConnection() {
    return false;
  }
  
  @Override
  public MongoOptions getMongoOptions() {
    return this.options ;
  }
}