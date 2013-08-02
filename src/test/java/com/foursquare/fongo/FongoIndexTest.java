package com.foursquare.fongo;

import com.foursquare.fongo.impl.Index;
import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FongoIndexTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testCreateIndexes() {
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
    collection.ensureIndex(new BasicDBObject("n", 1));
    collection.ensureIndex(new BasicDBObject("n", -1));
    List<DBObject> indexes = collection.getIndexInfo();
    assertEquals(
        Arrays.asList(
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", 1)).append("ns", "db.coll").append("name", "n_1"),
            new BasicDBObject("v", 1).append("key", new BasicDBObject("n", -1)).append("ns", "db.coll").append("name", "n_-1")
        ), indexes);
    Index index = getIndex(collection, "n_1");
    index = getIndex(collection, "n_-1");
  }

  @Test
  public void testDropIndex() {
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();
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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    FongoDBCollection collection = (FongoDBCollection) FongoTest.newCollection();

    collection.ensureIndex(new BasicDBObject("firstname", 1).append("lastname", 1));
    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.ensureIndex(new BasicDBObject("permalink", 1), "permalink_1", true);

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("firstname", "firstname" + i % 10).append("lastname", "lastname" + i % 10).append("date", i % 15).append("permalink", i));
    }

    Index indexFLname = getIndex(collection, "firstname_1_lastname_1");
    Index indexDate = getIndex(collection, "date_1");
    Index indexPermalink = getIndex(collection, "permalink_1");

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
    FongoDBCollection collection = (FongoDBCollection) FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    DBCollection collection = FongoTest.newCollection();

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
    FongoDBCollection collection = (FongoDBCollection) FongoTest.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 2));

    Index index = getIndex(collection, "date_1");
    assertEquals(0, index.size());

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1));
    assertEquals(1, index.size());
  }

  // Add or remove a field in an object must populate the index.
  @Test
  public void updateAndRemoveFieldMustAddIntoIndex() {
    FongoDBCollection collection = (FongoDBCollection) FongoTest.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1));

    Index index = getIndex(collection, "date_1");
    assertEquals(1, index.size());

    collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$unset", new BasicDBObject("date", 1)));
    assertEquals(0, index.size());
  }

  @Test
  public void indexesMustBeUsedForFindWithInFilter() {
    FongoDBCollection collection = (FongoDBCollection) FongoTest.newCollection();

    collection.ensureIndex(new BasicDBObject("date", 1));

    for (int i = 0; i < 20; i++) {
      collection.insert(new BasicDBObject("date", i % 10).append("_id", i));
    }

    Index indexDate = getIndex(collection, "date_1");

    assertEquals(0, indexDate.getLookupCount());

    List<DBObject> objects = collection.find(new BasicDBObject("date", new BasicDBObject("$in", Util.list(0, 1, 2)))).toArray();
    // No index used.
    assertEquals(1, indexDate.getLookupCount());
    assertEquals(6, objects.size());
  }

  @Test
  public void testFindOneOrData() {
    DBCollection collection = FongoTest.newCollection();
    collection.ensureIndex(new BasicDBObject("date", 1));
    collection.insert(new BasicDBObject("date", 1));
    DBObject result = collection.findOne(new BasicDBObject("$or", Util.list(new BasicDBObject("date", 1), new BasicDBObject("date", 2))));
    assertEquals(1, result.get("date"));
  }

  @Test
  public void testIdInQueryResultsInIndexOnFieldOrder() {
    DBCollection collection = FongoTest.newCollection();
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


  private Index getIndex(DBCollection collection, String name) {
    FongoDBCollection fongoDBCollection = (FongoDBCollection) collection;

    Index index = null;
    for (Index i : fongoDBCollection.getIndexes()) {
      if (i.getName().equals(name)) {
        index = i;
        break;
      }
    }
    assertNotNull("index not found :" + name, index);
    return index;
  }

}