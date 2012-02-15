package com.foursquare.fongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class PerTest {
  public static void main(String[] args) {
    doit();
    long startTime = System.currentTimeMillis();
    doit();
    System.out.println("Took " + (System.currentTimeMillis() - startTime));
  }

  public static void doit() {
    Fongo fongo = new Fongo("fongo", false);
    for (int i = 0; i < 1; i++){
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      for (int k = 0; k < 10000; k++){
        collection.update(new BasicDBObject("_id", k), new BasicDBObject("$push", new BasicDBObject("a", 1)), true, false);
      }
      db.dropDatabase();
    }
  }
}
