package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.foursquare.fongo.impl.index.IndexAbstract;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import com.mongodb.WriteConcernException;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class FongoIndexTest {

  public final FongoRule fongoRule = new FongoRule("db", !true);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(exception).around(fongoRule);

  @Test
  public void testCreateIndexes() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex("n");
    collection.ensureIndex("b");
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes);
  }

  /**
   * Same index = do not recreate.
   */
  @Test
  public void testCreateSameIndex() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex("n");
    collection.ensureIndex("n");
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1")
        ), indexes);
  }

  /**
   * Same index = do not recreate.
   */
  @Test
  public void testCreateSameIndexButUnique() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex(new BasicDBObject("n", 1), "n_1");
    collection.ensureIndex(new BasicDBObject("n", 1), "n_1", true);
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1")
        ), indexes);
  }

  @Test
  public void testCreateIndexOnSameFieldInversedOrder() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex(new BasicDBObject("n", 1));
    collection.ensureIndex(new BasicDBObject("n", -1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", -1)).append("ns", "db.coll").append("name", "n_-1")
        ), indexes);
  }

  @Test
  public void testDropIndexOnSameFieldInversedOrder() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex(new BasicDBObject("n", 1));
    collection.ensureIndex(new BasicDBObject("n", -1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", -1)).append("ns", "db.coll").append("name", "n_-1")
        ), indexes);
    IndexAbstract index = getIndex(collection, "n_1");
    index = getIndex(collection, "n_-1");
  }

  @Test
  public void testDropIndex() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex("n");
    collection.ensureIndex("b");

    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes);

    collection.dropIndex("n_1");
    indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes);
  }

  @Test
  public void testDropIndexes() {
    DBCollection collection = fongoRule.newCollection("coll");
    collection.ensureIndex("n");
    collection.ensureIndex("b");

    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("b", 1)).append("ns", "db.coll").append("name", "b_1")
        ), indexes);

    collection.dropIndexes();
    indexes = collection.getIndexInfo();
    assertEquals(0, indexes.size());
  }

  // Data are already here, but duplicated.
  @Test
  public void testCreateIndexOnDuplicatedData() {
    DBCollection collection = fongoRule.newCollection();

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 1));
    try {
      collection.ensureIndex(new BasicDBObject("n", 1), "n_1", true);
      fail("need MongoException on duplicate key.");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
      assertTrue(me.getMessage() + " doesn't contains " + "E11000 duplicate key error index: " + collection.getFullName() + ".n_1  dup key: { : [[1]] }",
          me.getMessage().contains("E11000 duplicate key error index: " + collection.getFullName() + ".n_1  dup key: { : [[1]] }")); // TODO [[ instead of " : \"1\""
    }
  }

  /**
   * Try to update an object and doesn't violate the unique index.
   */
  @Test
  public void testUpdateObjectOnUniqueIndexSameValue() {
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 2));

    assertEquals(1, collection.count(new BasicDBObject("n", 2)));
  }

  /**
   * Try to update an object and doesn't violate the unique index.
   */
  @Test
  public void testUpdateObjectOnUniqueIndexDifferentValue() {
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 3));

    assertEquals(0, collection.count(new BasicDBObject("n", 2)));
    assertEquals(1, collection.count(new BasicDBObject("n", 3)));
  }

  /**
   * Try to update an object but with same value as existing.
   */
  @Test
  public void testUpdateObjectOnUniqueIndex() {
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("n", 1), "n_1", true);

    collection.insert(new BasicDBObject("n", 1));
    collection.insert(new BasicDBObject("n", 2));

    // Update same.
    try {
      collection.update(new BasicDBObject("n", 2), new BasicDBObject("n", 1));
      fail("Must send MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    assertEquals(1, collection.count(new BasicDBObject("n", 2)));
    assertEquals(1, collection.count(new BasicDBObject("n", 1)));
  }

  @Test
  public void uniqueIndexesShouldNotPermitCreateOfDuplicatedEntries() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 2));
    try {
      collection.insert(new BasicDBObject("date", 1));
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }
  }

  @Test
  public void indexesShouldBeRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.ensureIndex(new BasicDBObject("field", 1), "fieldIndex");
    collection.ensureIndex(new BasicDBObject("string", 1), "stringIndex", true);

    List<DBObject> indexInfos = collection.getIndexInfo();
    assertEquals(3, indexInfos.size());
    assertEquals("date_1", indexInfos.get(0).get("name"));
    assertEquals("fieldIndex", indexInfos.get(1).get("name"));
    assertEquals("stringIndex", indexInfos.get(2).get("name"));

    collection.dropIndex("fieldIndex");
    indexInfos = collection.getIndexInfo();
    assertEquals(2, indexInfos.size());
    assertEquals("date_1", indexInfos.get(0).get("name"));
    assertEquals("stringIndex", indexInfos.get(1).get("name"));
  }

  @Test
  public void indexesMustBeUsedForFind() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("firstname", 1).append("lastname", 1));
    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.ensureIndex(new BasicDBObject("permalink", 1), "permalink_1", true);

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("firstname", "firstname" + i % 10).append("lastname", "lastname" + i % 10).append("date", i % 15).append("permalink", i));
    }

    IndexAbstract indexFLname = getIndex(collection, "firstname_1_lastname_1");
    IndexAbstract indexDate = getIndex(collection, "date_1");
    IndexAbstract indexPermalink = getIndex(collection, "permalink_1");

    assertEquals(0, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    collection.find(new BasicDBObject("firstname", "firstname0"));
    // No index used.
    assertEquals(0, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    List<DBObject> objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0")).toArray();
    assertEquals(2, objects.size());
    assertEquals(1, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0").append("date", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexPermalink.getLookupCount());

    objects = collection.find(new BasicDBObject("permalink", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(1, indexPermalink.getLookupCount());
  }

  // Check if index is correctly cleaned.
  @Test
  public void afterRemoveObjectMustNotBeRetrieved() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    collection.insert(new BasicDBObject("date", 1));

    List<DBObject> result = collection.find(new BasicDBObject("date", 1)).toArray();
    assertEquals(1, result.size());
    collection.remove(new BasicDBObject("date", 1));
    result = collection.find(new BasicDBObject("date", 1)).toArray();
    assertEquals(0, result.size());
  }

  @Test
  public void uniqueIndexesShouldNotPermitUpdateOfDuplicatedEntriesWhenUpdatedById() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    try {
      collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
      fail("should throw MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    // Verify object is NOT modify
    assertEquals(1, collection.count(new BasicDBObject("_id", 2)));
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }

  @Test
  public void uniqueIndexesShouldNotPermitCreateOfDuplicatedEntriesWhenUpdatedByField() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    try {
      collection.update(new BasicDBObject("date", 2), new BasicDBObject("date", 1));
      fail("should throw MongoException");
    } catch (MongoException me) {
      assertEquals(11000, me.getCode());
    }

    // Verify object is NOT modify
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }


  @Test
  public void uniqueIndexesCanPermitUpdateOfDuplicatedEntriesWhenUpdatedByIdTheSameObject() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    // Test
    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 2));

    // Verify object is NOT modified
    assertEquals(2, collection.find(new BasicDBObject("_id", 2)).next().get("date"));
  }

  @Test
  public void uniqueIndexesCanPermitCreateOfDuplicatedEntriesWhenUpdatedByFieldTheSameObject() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));
    collection.update(new BasicDBObject("date", 1), new BasicDBObject("date", 1));

    // Verify object is NOT modify
    assertEquals(1, collection.find(new BasicDBObject("_id", 1)).next().get("date"));
  }

  @Test
  public void uniqueIndexesShouldPermitCreateOfDuplicatedEntriesWhenIndexIsRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    collection.dropIndex("uniqueDate");

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("_id", 3).append("date", 1));
  }

  @Test
  public void uniqueIndexesShouldPermitCreateOfDuplicatedEntriesWhenAllIndexesAreRemoved() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true);

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));
    collection.insert(new BasicDBObject("_id", 2).append("date", 2));

    collection.dropIndex("uniqueDate");

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("_id", 3).append("date", 1));
  }

  // Add or remove a field in an object must populate the index.
  @Test
  public void updateAndAddFieldMustAddIntoIndex() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 2));

    IndexAbstract index = getIndex(collection, "date_1");
    assertEquals(0, index.size());

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    assertEquals(1, index.size());
  }

  // Add or remove a field in an object must populate the index.
  @Test
  public void updateAndRemoveFieldMustAddIntoIndex() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));

    IndexAbstract index = getIndex(collection, "date_1");
    assertEquals(1, index.size());

    collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$unset", new BasicDBObject("date", 1)));
    assertEquals(0, index.size());
  }

  @Test
  public void indexesMustBeUsedForFindWithInFilter() {
    DBCollection collection = fongoRule.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("date", i % 10).append("_id", i));
    }

    IndexAbstract indexDate = getIndex(collection, "date_1");

    assertEquals(0, indexDate.getLookupCount());

    List<DBObject> objects = collection.find(new BasicDBObject("date", new BasicDBObject("$in", Util.list(0, 1, 2)))).toArray();
    // No index used.
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(6, objects.size());
  }

  @Test
  public void testFindOneOrData() {
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 1));
    DBObject result = collection.findOne(new BasicDBObject("$or", Util.list(new BasicDBObject("date", 1), new BasicDBObject("date", 2))));
    assertEquals(1, result.get("date"));
  }

  @Test
  public void testIdInQueryResultsInIndexOnFieldOrder() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", 4));
    collection.insert(new BasicDBObject("date", 3));
    collection.insert(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 2));
    collection.ensureIndex(new BasicDBObject("date", 1));

    DBCursor cursor = collection.find(new BasicDBObject("date",
        new BasicDBObject("$in", Arrays.asList(3, 2, 1))), new BasicDBObject("date", 1).append("_id", 0));
    assertEquals(Arrays.asList(
        new BasicDBObject("date", 1),
        new BasicDBObject("date", 2),
        new BasicDBObject("date", 3)
    ), cursor.toArray());
  }

  @Test
  public void test2dIndex() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    IndexAbstract index = getIndex(collection, "loc_2d");
    assertTrue(index.isGeoIndex());
  }

  @Test(expected = WriteConcernException.class)
  public void test2dIndexNotFirst() {
    DBCollection collection = fongoRule.newCollection();
// com.mongodb.WriteConcernException: { "serverUsed" : "localhost/127.0.0.1:27017" , "err" : "2d has to be first in index" , "code" : 13023 , "n" : 0 , "connectionId" : 272 , "ok" : 1.0}

    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("name", 1).append("loc", "2d"));
  }

  @Test
  public void testUpdateMustModifyAllIndexes() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("date", 1).append("name", "jon").append("_id", 1));
    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.ensureIndex(new BasicDBObject("name", 1));

    IndexAbstract indexDate = getIndex(collection, "date_1");
    IndexAbstract indexName = getIndex(collection, "name_1");

    // Now, modify an object.
    collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("name", "will")));
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(0, indexName.getLookupCount());

    // Find in with index "name"
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("name", "will")));
    assertEquals(1, indexName.getLookupCount());
    assertEquals(0, indexDate.getLookupCount());
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("date", 1)));
    assertEquals(1, indexName.getLookupCount());
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(new BasicDBObject("date", 1).append("name", "will").append("_id", 1), collection.findOne(new BasicDBObject("_id", 1)));
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(1, indexName.getLookupCount());
  }

  @Test
  @Ignore("strange index, Mongo doen't handle but no exception.")
  public void testInnerIndex() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)));

    assertEquals(
        new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)),
        collection.findOne(new BasicDBObject("a.n", 1))
    );

    collection.ensureIndex(new BasicDBObject("a.n", 1));
    IndexAbstract index = getIndex(collection, "a.n_1");
    assertEquals(
        new BasicDBObject("_id", 1).append("a", new BasicDBObject("n", 1)),
        collection.findOne(new BasicDBObject("a.n", 1))
    );
    assertEquals(1, index.getLookupCount());
  }

  @Test
  public void testStrangeIndexThrowException() throws Exception {
    exception.expect(MongoException.class);
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("a", new BasicDBObject("n", 1)));

    // Code : 10098
  }

  // Creating an index after inserting into a collection must add records only if necessary
  @Test
  public void testCreateIndexLater() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.ensureIndex(new BasicDBObject("a", 1));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(1, index.size());
  }

  // Creating an index before inserting into a collection must add records only if necessary
  @Test
  public void testCreateIndexBefore() throws Exception {
    DBCollection collection = FongoTest.newCollection();
    collection.ensureIndex(new BasicDBObject("a", 1));
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(1, index.size());
  }

  @Test
  public void testRemoveMulti() throws Exception {
    DBCollection collection = FongoTest.newCollection();
    collection.ensureIndex(new BasicDBObject("a", 1));
    collection.insert(new BasicDBObject("_id", 1).append("a", 1));
    collection.insert(new BasicDBObject("_id", 2));
    collection.insert(new BasicDBObject("_id", 3).append("a", 1));

    IndexAbstract index = getIndex(collection, "a_1");
    assertEquals(2, index.size());

    collection.remove(new BasicDBObject("a", 1));
    assertEquals(0, index.size());
  }

  static IndexAbstract getIndex(DBCollection collection, String name) {
    FongoDBCollection fongoDBCollection = (FongoDBCollection) collection;

    IndexAbstract index = null;
    for (IndexAbstract i : fongoDBCollection.getIndexes()) {
      if (i.getName().equals(name)) {
        index = i;
        break;
      }
    }
    assertNotNull("index not found :" + name, index);
    return index;
  }

}
