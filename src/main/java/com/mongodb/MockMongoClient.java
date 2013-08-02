package com.mongodb;

import com.foursquare.fongo.Fongo;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import org.objenesis.ObjenesisStd;

public class MockMongoClient extends MongoClient {

  // this is immutable 
  private final static MongoClientOptions clientOptions = MongoClientOptions.builder().build();

  private Fongo fongo;
  private MongoOptions options;

  public static MockMongoClient create(Fongo fongo) {
    // using objenesis here to prevent default constructor from spinning up background threads.
    MockMongoClient client = (MockMongoClient) new ObjenesisStd().getInstantiatorOf(MockMongoClient.class).newInstance();
    client.options = new MongoOptions(clientOptions);
    client.fongo = fongo;
    client.setWriteConcern(clientOptions.getWriteConcern());
    return client;
  }

  public MockMongoClient() throws UnknownHostException {

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
    return options;
  }

  @Override
  public MongoClientOptions getMongoClientOptions() {
    return clientOptions;
  }
}
