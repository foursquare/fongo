package com.foursquare.fongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

public class FongoRule extends ExternalResource {

  /**
   * Will be true if we use the real MongoDB to test things against real world.
   */
  private final boolean realMongo;

  private Mongo mongo;

  private DB db;

  /**
   * Setup a rule with a real MongoDB.
   *
   * @param realMongo
   */
  public FongoRule(boolean realMongo) {
    this.realMongo = realMongo;
  }

  public FongoRule() {
    this(false);
  }

  @Override
  protected void before() throws UnknownHostException {
    if (realMongo) {
      mongo = new MongoClient();
    } else {
      mongo = newFongo().getMongo();
    }
    db = mongo.getDB(UUID.randomUUID().toString());
  }

  @Override
  protected void after() {
    db.dropDatabase();
  }

  public DBCollection newCollection() {
    DBCollection collection = db.getCollection(UUID.randomUUID().toString());
    return collection;
  }

  public Fongo newFongo() {
    Fongo fongo = new Fongo("test");
    return fongo;
  }

}
