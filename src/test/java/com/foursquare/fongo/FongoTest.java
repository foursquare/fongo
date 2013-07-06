package com.foursquare.fongo;

import org.bson.BSON;
import org.bson.Transformer;
import org.bson.types.ObjectId;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FongoTest {

  @Test
  public void testGetDb() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    assertNotNull(db);
    assertSame("getDB should be idempotent", db, fongo.getDB("db"));
    assertEquals(Arrays.asList(db), fongo.getUsedDatabases());
    assertEquals(Arrays.asList("db"), fongo.getDatabaseNames());
  }
  
  @Test
  public void testGetCollection() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection collection = db.getCollection("coll");
    assertNotNull(collection);
    assertSame("getCollection should be idempotent", collection, db.getCollection("coll"));
    assertSame("getCollection should be idempotent", collection, db.getCollectionFromString("coll"));
    assertEquals(new HashSet<String>(Arrays.asList("coll")), db.getCollectionNames());
  }
  
  @Test
  public void testCreateCollection() {
      Fongo fongo = newFongo();
      DB db = fongo.getDB("db");
      db.createCollection("coll", null);
      assertEquals(Collections.singleton("coll"), db.getCollectionNames());
  }

  @Test 
  public void testCountMethod() {
    DBCollection collection = newCollection();
    assertEquals(0, collection.count());
  }
  
  @Test 
  public void testCountWithQueryCommand() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));
    collection.insert(new BasicDBObject("n", 2));
    assertEquals(2, collection.count(new BasicDBObject("n", 2)));
  }
  
  @Test
  public void testCountOnCursor() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));
    collection.insert(new BasicDBObject("n", 2));
    assertEquals(3,collection.find(QueryBuilder.start("n").exists(true).get()).count());
  }
  
  @Test
  public void testInsertIncrementsCount() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("name", "jon"));
    assertEquals(1, collection.count());
  }
  
  @Test
  public void testFindOne() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("name", "jon"));
    DBObject result = collection.findOne();
    assertNotNull(result);
    assertNotNull("should have an _id", result.get("_id"));
  }
  
  @Test
  public void testFindOneNoData() {
    DBCollection collection = newCollection();
    DBObject result = collection.findOne();
    assertNull(result);
  }
  
  @Test
  public void testFindOneIn() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    DBObject result = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1,2))));
    assertEquals(new BasicDBObject("_id", 1), result);
  }

  @Test
  public void testFindOneInWithArray() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));

    DBObject result = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$in", new Integer[] {1, 3})));
    assertEquals(new BasicDBObject("_id", 1), result);
  }

  @Test
  public void testFindOneNinWithArray() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));

    DBObject result = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$nin", new Integer[] {1, 3})));
    assertEquals(new BasicDBObject("_id", 2), result);
  }

  @Test
  public void testFindOneById() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    DBObject result = collection.findOne(new BasicDBObject("_id", 1));
    assertEquals(new BasicDBObject("_id", 1), result);
    
    assertEquals(null, collection.findOne(new BasicDBObject("_id", 2)));
  }

  @Test
  public void testFindWithQuery() {
    DBCollection collection = newCollection();

    collection.insert(new BasicDBObject("name", "jon"));
    collection.insert(new BasicDBObject("name", "leo"));
    collection.insert(new BasicDBObject("name", "neil"));
    collection.insert(new BasicDBObject("name", "neil"));
    DBCursor cursor = collection.find(new BasicDBObject("name", "neil"));
    assertEquals("should have two neils", 2, cursor.toArray().size());
  }

  @Test
  public void testFindWithNullOrNoFieldFilter() {
    DBCollection collection = newCollection();

    collection.insert(new BasicDBObject("name", "jon").append("group", "group1"));
    collection.insert(new BasicDBObject("name", "leo").append("group", "group1"));
    collection.insert(new BasicDBObject("name", "neil1").append("group", "group2"));
    collection.insert(new BasicDBObject("name", "neil2").append("group", null));
    collection.insert(new BasicDBObject("name", "neil3"));

    // check {group: null} vs {group: {$exists: false}} filter
    DBCursor cursor1 = collection.find(new BasicDBObject("group", null));
    assertEquals("should have two neils (neil2, neil3)", 2, cursor1.toArray().size());

    DBCursor cursor2 = collection.find(new BasicDBObject("group", new BasicDBObject("$exists", false)));
    assertEquals("should have one neil (neil3)", 1, cursor2.toArray().size());

    // same check but for fields which don't exist in DB
    DBCursor cursor3 = collection.find(new BasicDBObject("other", null));
    assertEquals("should return all documents", 5, cursor3.toArray().size());

    DBCursor cursor4 = collection.find(new BasicDBObject("other", new BasicDBObject("$exists", false)));
    assertEquals("should return all documents", 5, cursor4.toArray().size());
  }

  @Test
  public void testFindWithLimit() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4));

    DBCursor cursor = collection.find().limit(2);
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1),
        new BasicDBObject("_id", 2)
    ), cursor.toArray());
  }
  
  @Test
  public void testFindWithSkipLimit() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4));

    DBCursor cursor = collection.find().limit(2).skip(2);
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 3),
        new BasicDBObject("_id", 4)
    ), cursor.toArray());
  }
  
  @Test
  public void testIdInQueryResultsInIndexOrder() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 4));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));

    DBCursor cursor = collection.find(new BasicDBObject("_id", 
        new BasicDBObject("$in", Arrays.asList(3,2,1))));
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1),
        new BasicDBObject("_id", 2),
        new BasicDBObject("_id", 3)
    ), cursor.toArray());
  }
  
  @Test
  public void testSort() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("a", 1).append("_id", 1));
    collection.insert(new BasicDBObject("a", 2).append("_id", 2));
    collection.insert(new BasicDBObject("_id", 5));
    collection.insert(new BasicDBObject("a", 3).append("_id", 3));
    collection.insert(new BasicDBObject("a", 4).append("_id", 4));

    DBCursor cursor = collection.find().sort(new BasicDBObject("a", -1));
    assertEquals(Arrays.asList(
        new BasicDBObject("a", 4).append("_id", 4),
        new BasicDBObject("a", 3).append("_id", 3),
        new BasicDBObject("a", 2).append("_id", 2),
        new BasicDBObject("a", 1).append("_id", 1),
        new BasicDBObject("_id", 5)
    ), cursor.toArray());
  }
  
  @Test
  public void testCompoundSort() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("a", 1).append("_id", 1));
    collection.insert(new BasicDBObject("a", 2).append("_id", 5));
    collection.insert(new BasicDBObject("a", 1).append("_id", 2));
    collection.insert(new BasicDBObject("a", 2).append("_id", 4));
    collection.insert(new BasicDBObject("a", 1).append("_id", 3));

    DBCursor cursor = collection.find().sort(new BasicDBObject("a", 1).append("_id", -1));
    assertEquals(Arrays.asList(
        new BasicDBObject("a", 1).append("_id", 3),
        new BasicDBObject("a", 1).append("_id", 2),
        new BasicDBObject("a", 1).append("_id", 1),
        new BasicDBObject("a", 2).append("_id", 5),
        new BasicDBObject("a", 2).append("_id", 4)
    ), cursor.toArray());
  }
  
  @Test
  public void testEmbeddedSort() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4).append("counts", new BasicDBObject("done", 1)));
    collection.insert(new BasicDBObject("_id", 5).append("counts", new BasicDBObject("done", 2)));

    DBCursor cursor = collection.find(new BasicDBObject("c", new BasicDBObject("$ne", true))).sort(new BasicDBObject("counts.done", -1));
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 5).append("counts", new BasicDBObject("done", 2)),
        new BasicDBObject("_id", 4).append("counts", new BasicDBObject("done", 1)),
        new BasicDBObject("_id", 1),
        new BasicDBObject("_id", 2),
        new BasicDBObject("_id", 3)
    ), cursor.toArray());
  }
  

  @Test
  public void testBasicUpdate() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2).append("b", 5));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4));
    
    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("a", 5));
    
    assertEquals(new BasicDBObject("_id", 2).append("a", 5), 
        collection.findOne(new BasicDBObject("_id",2)));
  }

  @Test
  public void testFullUpdateWithSameId() throws Exception {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2).append("b", 5));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4));

    collection.update(
        new BasicDBObject("_id", 2).append("b", 5),
        new BasicDBObject("_id", 2).append("a", 5));

    assertEquals(new BasicDBObject("_id", 2).append("a", 5),
            collection.findOne(new BasicDBObject("_id", 2)));
  }

  @Test
  public void testIdNotAllowedToBeUpdated() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    
    try {
      collection.update(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2).append("a", 5));
      fail("should throw exception");
    } catch (MongoException e) {
      
    }
  }
  
  @Test
  public void testUpsert() {
    DBCollection collection = newCollection();
    WriteResult result = collection.update(new BasicDBObject("_id", 1).append("n", "jon"), 
        new BasicDBObject("$inc", new BasicDBObject("a", 1)), true, false);
    assertEquals(new BasicDBObject("_id", 1).append("n", "jon").append("a", 1),
        collection.findOne());
    assertFalse(result.getLastError().getBoolean("updatedExisting"));
  }
  
  @Test
  public void testUpsertExisting() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    WriteResult result = collection.update(new BasicDBObject("_id", 1), 
        new BasicDBObject("$inc", new BasicDBObject("a", 1)), true, false);
    assertEquals(new BasicDBObject("_id", 1).append("a", 1),
        collection.findOne());
    assertTrue(result.getLastError().getBoolean("updatedExisting"));
  }
  
  @Test
  public void testUpsertWithConditional() {
    DBCollection collection = newCollection();
    collection.update(new BasicDBObject("_id", 1).append("b", new BasicDBObject("$gt", 5)), 
        new BasicDBObject("$inc", new BasicDBObject("a", 1)), true, false);
    assertEquals(new BasicDBObject("_id", 1).append("a", 1),
        collection.findOne());
  }
  
  @Test
  public void testUpsertWithIdIn() {
    DBCollection collection = newCollection();
    DBObject query = new BasicDBObjectBuilder().push("_id").append("$in", Arrays.asList(1)).pop().get();
    DBObject update = new BasicDBObjectBuilder()
      .push("$push").push("n").append("_id", 2).append("u", 3).pop().pop()
      .push("$inc").append("c",4).pop().get();
    DBObject expected = new BasicDBObjectBuilder().append("_id", 1).append("n", Arrays.asList(new BasicDBObject("_id", 2).append("u", 3))).append("c", 4).get();
    collection.update(query, update, true, false);
    assertEquals(expected, collection.findOne());
  }
  
  @Test
  public void testUpdateWithIdIn() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    DBObject query = new BasicDBObjectBuilder().push("_id").append("$in", Arrays.asList(1)).pop().get();
    DBObject update = new BasicDBObjectBuilder()
      .push("$push").push("n").append("_id", 2).append("u", 3).pop().pop()
      .push("$inc").append("c",4).pop().get();
    DBObject expected = new BasicDBObjectBuilder().append("_id", 1).append("n", Arrays.asList(new BasicDBObject("_id", 2).append("u", 3))).append("c", 4).get();
    collection.update(query, update, false, true);
    assertEquals(expected, collection.findOne());
  }
  
  @Test
  public void testUpdateWithObjectId() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
    DBObject query = new BasicDBObject("_id", new BasicDBObject("n", 1));
    DBObject update = new BasicDBObject("$set", new BasicDBObject("a", 1));
    collection.update(query, update, false, false);
    assertEquals(new BasicDBObject("_id", new BasicDBObject("n", 1)).append("a", 1), collection.findOne());
  }
  
  @Test
  public void testUpdateWithIdInMulti() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1),new BasicDBObject("_id", 2));
    collection.update(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1,2))), 
        new BasicDBObject("$set", new BasicDBObject("n", 1)), false, true);
    List<DBObject> results = collection.find().toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1).append("n", 1),
        new BasicDBObject("_id", 2).append("n", 1)
    ), results);
    
  }
  
  @Test
  public void testUpdateWithIdQuery() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1),new BasicDBObject("_id", 2));
    collection.update(new BasicDBObject("_id", new BasicDBObject("$gt", 1)), 
        new BasicDBObject("$set", new BasicDBObject("n", 1)), false, true);
    List<DBObject> results = collection.find().toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1),
        new BasicDBObject("_id", 2).append("n", 1)
    ), results);
    
  }
  
  @Test
  public void testCompoundDateIdUpserts(){
    DBCollection collection = newCollection();
    DBObject query = new BasicDBObjectBuilder().push("_id")
      .push("$lt").add("n", "a").add("t", 10).pop()
      .push("$gte").add("n", "a").add("t", 1).pop()
      .pop().get();
    List<BasicDBObject> toUpsert = Arrays.asList(
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 1)),
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 2)),
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 3)),
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 11))
    );
    for (BasicDBObject dbo : toUpsert) {
      collection.update(dbo, ((BasicDBObject)dbo.copy()).append("foo", "bar"), true, false);
    }
    System.out.println(collection.find().toArray());
    List<DBObject> results = collection.find(query).toArray();
    assertEquals(Arrays.<DBObject>asList(
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 1)).append("foo", "bar"),
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 2)).append("foo", "bar"),
        new BasicDBObject("_id", new BasicDBObject("n","a").append("t", 3)).append("foo", "bar")
    ), results);
  }
  
  @Test
  public void testAnotherUpsert() {
    DBCollection collection = newCollection();
    BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start().push("_id").
        append("f", "ca").push("1").append("l", 2).pop().push("t").append("t", 11).pop().pop();
    DBObject query = queryBuilder.get();
    
    DBObject update = BasicDBObjectBuilder.start().push("$inc").append("n.!", 1).append("n.a.b:false", 1).pop().get();
    collection.update(query, update, true, false);
    
    DBObject expected = queryBuilder.push("n").append("!", 1).push("a").append("b:false", 1).pop().pop().get();
    assertEquals(expected, collection.findOne());
  }
  
  @Test
  public void testUpsertOnIdWithPush() {
    DBCollection collection = newCollection();

    DBObject update1 = BasicDBObjectBuilder.start().push("$push")
        .push("c").append("a",1).append("b", 2).pop().pop().get();
    
    DBObject update2 = BasicDBObjectBuilder.start().push("$push")
        .push("c").append("a",3).append("b", 4).pop().pop().get();

    collection.update(new BasicDBObject("_id", 1), update1, true, false);
    collection.update(new BasicDBObject("_id", 1), update2, true, false);
    
    DBObject expected = new BasicDBObject("_id", 1).append("c", Util.list(
        new BasicDBObject("a", 1).append("b", 2),
        new BasicDBObject("a", 3).append("b", 4)));
    
    assertEquals(expected, collection.findOne(new BasicDBObject("c.a", 3).append("c.b", 4)));
  }
  
  @Test
  public void testUpsertWithEmbeddedQuery() {
    DBCollection collection = newCollection();

    DBObject update = BasicDBObjectBuilder.start().push("$set").append("a",1).pop().get();
    
    collection.update(new BasicDBObject("_id", 1).append("e.i", 1), update, true, false);
    
    DBObject expected = BasicDBObjectBuilder.start().append("_id", 1).push("e").append("i", 1).pop().append("a", 1).get();
    
    assertEquals(expected, collection.findOne(new BasicDBObject("_id", 1)));
  }
  
  @Test
  public void testFindAndModifyReturnOld() {
    final DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", 1).append("b", new BasicDBObject("c", 1)));

    final BasicDBObject query = new BasicDBObject("_id", 1);
    final BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject("a", 1).append("b.c", 1));
    System.out.println("update: " + update);
    final DBObject result = collection.findAndModify(query, null, null, false, update, false, false);

    assertEquals(new BasicDBObject("_id", 1).append("a", 1).append("b", new BasicDBObject("c", 1)), result);
    assertEquals(new BasicDBObject("_id", 1).append("a", 2).append("b", new BasicDBObject("c", 2)), collection.findOne());
  }

  @Test
  public void testFindAndModifyReturnNew() {
    final DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", 1).append("b", new BasicDBObject("c", 1)));

    final BasicDBObject query = new BasicDBObject("_id", 1);
    final BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject("a", 1).append("b.c", 1));
    System.out.println("update: " + update);
    final DBObject result = collection.findAndModify(query, null, null, false, update, true, false);

    assertEquals(new BasicDBObject("_id", 1).append("a", 2).append("b", new BasicDBObject("c", 2)), result);
  }
  
  @Test
  public void testFindAndModifyUpsert() {
    DBCollection collection = newCollection();
    
    DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), 
        null, null, false, new BasicDBObject("$inc", new BasicDBObject("a", 1)), true, true);
    
    assertEquals(new BasicDBObject("_id", 1).append("a", 1), result);
    assertEquals(new BasicDBObject("_id", 1).append("a", 1), collection.findOne());
  }
  
  @Test
  public void testFindAndModifyUpsertReturnNewFalse() {
    DBCollection collection = newCollection();
    
    DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), 
        null, null, false, new BasicDBObject("$inc", new BasicDBObject("a", 1)), false, true);
    
    assertEquals(new BasicDBObject(), result);
    assertEquals(new BasicDBObject("_id", 1).append("a", 1), collection.findOne());
  }
  
  @Test
  public void testFindAndRemoveFromEmbeddedList() {
    DBCollection collection = newCollection();
    BasicDBObject obj = new BasicDBObject("_id", 1).append("a", Arrays.asList(1));
    collection.insert(obj);
    DBObject result = collection.findAndRemove(new BasicDBObject("_id",1));
    assertEquals(obj, result);
  }
  
  @Test
  public void testFindAndModifyRemove() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    DBObject result = collection.findAndModify(new BasicDBObject("_id", 1), 
        null, null, true, null, false, false);
    
    assertEquals(new BasicDBObject("_id", 1).append("a", 1), result);
    assertEquals(null, collection.findOne());
  }
  
  @Test
  public void testRemove() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3));
    collection.insert(new BasicDBObject("_id", 4));
    
    collection.remove(new BasicDBObject("_id", 2));
    
    assertEquals(null, 
        collection.findOne(new BasicDBObject("_id",2)));
  }
  
  @Test
  public void testConvertJavaListToDbList() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("n", Arrays.asList(1,2)));
    DBObject result = collection.findOne();
    assertTrue("not a DBList", result.get("n") instanceof BasicDBList);
    
  }
  
  @Test
  public void testConvertJavaMapToDBObject() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("n", Collections.singletonMap("a", 1)));
    DBObject result = collection.findOne();
    assertTrue("not a DBObject", result.get("n") instanceof BasicDBObject);
    
  }
  
  @Test
  public void testDistinctQuery() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("n", 1).append("_id", 1));
    collection.insert(new BasicDBObject("n", 2).append("_id", 2));
    collection.insert(new BasicDBObject("n", 3).append("_id", 3));
    collection.insert(new BasicDBObject("n", 1).append("_id", 4));
    collection.insert(new BasicDBObject("n", 1).append("_id", 5));
    assertEquals(Arrays.asList(1, 2, 3), collection.distinct("n"));
  }
  
  @Test
  public void testGetLastError(){
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection collection = db.getCollection("coll");
    collection.insert(new BasicDBObject("_id", 1));
    CommandResult error = db.getLastError();
    assertTrue(error.ok());
  }
  
  @Test
  public void testSave() {
    DBCollection collection = newCollection();
    BasicDBObject inserted = new BasicDBObject("_id", 1);
    collection.insert(inserted);
    collection.save(inserted);
  }
  
  @Test(expected = MongoException.DuplicateKey.class)
  public void testInsertDuplicateWithConcernThrows(){
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 1), WriteConcern.SAFE);
  }
  
  @Test(expected = MongoException.DuplicateKey.class)
  public void testInsertDuplicateWithConcernOnMongo(){
    DBCollection collection = newCollection();
    collection.getDB().getMongo().setWriteConcern(WriteConcern.ACKNOWLEDGED);
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 1));
  }

  @Test
  public void testInsertDuplicateIgnored(){
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 1));
    assertEquals(1, collection.count());
  }
  
  @Test
  public void testCreateIndexes() {
    DBCollection collection = newCollection();
    collection.ensureIndex("n");
    collection.ensureIndex("b");
    List<DBObject> indexes = collection.getDB().getCollection("system.indexes").find().toArray();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes);
  }
  
  @Test
  public void testSortByEmeddedKey(){
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", new BasicDBObject("b", 1)));
    collection.insert(new BasicDBObject("_id", 2).append("a", new BasicDBObject("b", 2)));
    collection.insert(new BasicDBObject("_id", 3).append("a", new BasicDBObject("b", 3)));
    List<DBObject> results = collection.find().sort(new BasicDBObject("a.b", -1)).toArray();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("_id", 3).append("a", new BasicDBObject("b", 3)),
            new BasicDBObject("_id", 2).append("a", new BasicDBObject("b", 2)),
            new BasicDBObject("_id", 1).append("a", new BasicDBObject("b", 1))
        ),results);
  }

  @Test
  public void testInsertReturnModifiedDocumentCount() {
    DBCollection collection = newCollection();
    WriteResult result = collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
    assertEquals(1, result.getN());
  }
  
  @Test
  public void testUpdateWithIdInMultiReturnModifiedDocumentCount() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1),new BasicDBObject("_id", 2));
    WriteResult result = collection.update(new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList(1,2))), 
        new BasicDBObject("$set", new BasicDBObject("n", 1)), false, true);
    assertEquals(2, result.getN());
  }
  
  @Test
  public void testUpdateWithObjectIdReturnModifiedDocumentCount() {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", new BasicDBObject("n", 1)));
    DBObject query = new BasicDBObject("_id", new BasicDBObject("n", 1));
    DBObject update = new BasicDBObject("$set", new BasicDBObject("a", 1));
    WriteResult result = collection.update(query, update, false, false);
    assertEquals(1, result.getN());
  }

  /**
   * Test that ObjectId is getting generated even if _id is present in
   * DBObject but it's value is null
   *
   * @throws Exception
   */
  @Test
  public void testIdGenerated() throws Exception {
    DBObject toSave = new BasicDBObject();
    toSave.put("_id", null);
    toSave.put("name", "test");
    Fongo fongo = newFongo();
    DB fongoDB = fongo.getDB("testDB");
    DBCollection collection = fongoDB.getCollection("testCollection");
    collection.save(toSave);
    DBObject result = collection.findOne(new BasicDBObject("name", "test"));
    //default index in mongoDB
    final String ID_KEY = "_id";
    assertNotNull("Expected _id to be generated" + result.get(ID_KEY));
  }
  
  @Test
  public void testDropDatabaseAlsoDropsCollectionData() throws Exception {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject());
    collection.getDB().dropDatabase();
    assertEquals("Collection should have no data", 0, collection.count());
  }
  
  @Test
  public void testDropCollectionAlsoDropsFromDB() throws Exception {
    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject());
    collection.drop();
    assertEquals("Collection should have no data", 0, collection.count());
    assertFalse("Collection shouldn't exist in DB", collection.getDB().getCollectionNames().contains(collection.getName()));
  }
  
  @Test
  public void testDropDatabaseFromFongoDropsAllData() throws Exception {
    Fongo fongo = newFongo();
    DBCollection collection = fongo.getDB("db").getCollection("coll");
    collection.insert(new BasicDBObject());
    fongo.dropDatabase("db");
    assertEquals("Collection should have no data", 0, collection.count());
    assertFalse("Collection shouldn't exist in DB", collection.getDB().getCollectionNames().contains(collection.getName()));
    assertFalse("DB shouldn't exist in fongo", fongo.getDatabaseNames().contains("db"));
  }

  @Test
  public void testDropDatabaseFromFongoWithMultipleCollectionsDropsBothCollections() throws Exception {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection collection1 = db.getCollection("coll1");
    DBCollection collection2= db.getCollection("coll2");
    db.dropDatabase();
    assertFalse("Collection 1 shouldn't exist in DB", db.collectionExists(collection1.getName()));
    assertFalse("Collection 2 shouldn't exist in DB", db.collectionExists(collection2.getName()));
    assertFalse("DB shouldn't exist in fongo", fongo.getDatabaseNames().contains("db"));
  }
  
  @Test
  public void testDropCollectionsFromGetCollectionNames() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    db.getCollection("coll1");
    db.getCollection("coll2");
    int dropCount = 0;
    for (String name : db.getCollectionNames()){
      db.getCollection(name).drop();
      dropCount++;
    }
    assertEquals("should drop two collections", 2, dropCount);
  }

  @Test
  public void testToString() {
    new Fongo("test").getMongo().toString();
  }
  
  @Test
  public void testUndefinedCommand() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    CommandResult result = db.command("undefined");
    assertEquals("ok should always be defined", false, result.get("ok"));
    assertEquals("undefined command: { \"undefined\" : true}", result.get("err"));
  }
  
  @Test
  public void testCountCommand() {
    DBObject countCmd = new BasicDBObject("count", "coll");
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection coll = db.getCollection("coll");
    coll.insert(new BasicDBObject());
    coll.insert(new BasicDBObject());
    CommandResult result = db.command(countCmd);
    assertEquals("The command should have been succesful", true, result.get("ok"));
    assertEquals("The count should be in the result", 2L, result.get("n"));
  }
  
  @Test
  public void testExplicitlyAddedObjectIdNotNew() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection coll = db.getCollection("coll");
    ObjectId oid = new ObjectId();
    assertTrue("new should be true", oid.isNew());
    coll.save(new BasicDBObject("_id", oid ));
    ObjectId retrievedOid = (ObjectId) coll.findOne().get("_id");
    assertEquals("retrieved should still equal the inserted", oid, retrievedOid);
    assertFalse("retrieved should not be new", retrievedOid.isNew());
  }
  
  @Test
  public void testAutoCreatedObjectIdNotNew() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection coll = db.getCollection("coll");
    coll.save(new BasicDBObject());
    ObjectId retrievedOid = (ObjectId) coll.findOne().get("_id");
    assertFalse("retrieved should not be new", retrievedOid.isNew());
  }
  
  @Test
  public void testDbRefs(){
    Fongo fong = newFongo();
    DB db = fong.getDB("db");
    DBCollection coll1 = db.getCollection("coll");
    DBCollection coll2 = db.getCollection("coll2");
    final String coll2oid = "coll2id";
    BasicDBObject coll2doc = new BasicDBObject("_id", coll2oid);
    coll2.insert(coll2doc);
    coll1.insert(new BasicDBObject("ref", new DBRef(db, "coll2", coll2oid)));
    
    DBRef ref = (DBRef)coll1.findOne().get("ref");
    assertEquals("db", ref.getDB().getName());
    assertEquals("coll2", ref.getRef());
    assertEquals(coll2oid, ref.getId());
    assertEquals(coll2doc, ref.fetch());
  }

  @Test
  public void testEncodingHooks() {
    BSON.addEncodingHook(Seq.class, new Transformer() {
      @Override
      public Object transform(Object o) {
        return (o instanceof Seq) ? ((Seq) o).data : o;
      }
    });

    DBCollection collection = newCollection();
    collection.insert(new BasicDBObject("_id", 1));
    collection.insert(new BasicDBObject("_id", 2));

    DBObject result1 = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$in", new Seq(1, 3))));
    assertEquals(new BasicDBObject("_id", 1), result1);

    DBObject result2 = collection.findOne(new BasicDBObject("_id", new BasicDBObject("$nin", new Seq(1, 3))));
    assertEquals(new BasicDBObject("_id", 2), result2);
  }

  class Seq {
    Object[] data;
    Seq(Object... data) { this.data = data; }
  }

  private DBCollection newCollection() {
    Fongo fongo = newFongo();
    DB db = fongo.getDB("db");
    DBCollection collection = db.getCollection("coll");
    return collection;
  }

  public Fongo newFongo() {
    Fongo fongo = new Fongo("test");
    return fongo;
  }

}
