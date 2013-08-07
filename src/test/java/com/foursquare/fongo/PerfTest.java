package com.foursquare.fongo;

import ch.qos.logback.classic.Level;
import com.foursquare.fongo.impl.ExpressionParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.FongoDBCollection;
import org.slf4j.LoggerFactory;

public class PerfTest {
  public static void main(String[] args) {
    // Desactivate logback
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FongoDBCollection.class);
    log.setLevel(Level.ERROR);
    log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ExpressionParser.class);
    log.setLevel(Level.ERROR);

    System.out.println("Warming jvm");
    // Microbenchmark warm
    for (int i = 0; i < 10000; i++) {
      doit(100);
      doitFindN(100);
      doitIndexes(100);
      doitFindNWithIndex(100);
    }
    System.out.println("Warming jvm done.");
    long startTime = System.currentTimeMillis();
    doit(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms");

    startTime = System.currentTimeMillis();
    doitIndexes(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with one useless index.");

    startTime = System.currentTimeMillis();
    doitFindN(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with no index.");

    startTime = System.currentTimeMillis();
    doitFindNWithIndex(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with index.");
  }

  public static void doit(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
        collection.findOne(new BasicDBObject("_id", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitFindN(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", k)));
        collection.findOne(new BasicDBObject("n.a", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitIndexes(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      collection.ensureIndex(new BasicDBObject("n.a", 1));
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
        collection.findOne(new BasicDBObject("_id", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitFindNWithIndex(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      collection.ensureIndex(new BasicDBObject("n.a", 1));
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", k)));
        collection.findOne(new BasicDBObject("n.a", k));
      }
      db.dropDatabase();
    }
  }
}
