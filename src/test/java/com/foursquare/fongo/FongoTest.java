package com.foursquare.fongo;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class FongoTest {

  @Test
  public void testGetDb() {
    Fongo fongo = new Fongo();
    DB db = fongo.getDB("db");
    assertNotNull(db);
    assertSame("getDB should be idempotent", db, fongo.getDB("db"));
    assertEquals(Arrays.asList(db), fongo.getUsedDatabases());
    assertEquals(Arrays.asList("db"), fongo.getDatabaseNames());
  }
  
  @Test
  public void testGetCollection() {
    Fongo fongo = new Fongo();
    DB db = fongo.getDB("db");
    DBCollection collection = db.getCollection("coll");
    assertNotNull(collection);
    assertSame("getCollection should be idempotent", collection, db.getCollection("coll"));
    assertSame("getCollection should be idempotent", collection, db.getCollectionFromString("coll"));
    assertEquals(new HashSet<String>(Arrays.asList("coll")), db.getCollectionNames());
  }
  
  @Test 
  public void testCountCommand() {
    DBCollection collection = newCollection();
    assertEquals(0, collection.count());
  }
  
  @Test
  public void testInsertIncrementsCount() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("name", "jon"));
    assertEquals(1, collection.count());
  }

  private DBCollection newCollection() {
    Fongo fongo = new Fongo();
    DB db = fongo.getDB("db");
    DBCollection collection = db.getCollection("coll");
    return collection;
  }

}
