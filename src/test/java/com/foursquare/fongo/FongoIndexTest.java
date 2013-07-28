package com.foursquare.fongo;

import com.foursquare.fongo.impl.Index;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
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

    Index indexFLname = null;
    Index indexDate = null;
    Index indexPermalink = null;
    for (Index index : collection.getIndexes()) {
      if (index.getName().equals("firstname_1_lastname_1")) {
        indexFLname = index;
      } else if (index.getName().equals("permalink_1")) {
        indexPermalink = index;
      } else {
        indexDate = index;
      }
    }

    assertNotNull(indexFLname);
    assertNotNull(indexDate);
    assertNotNull(indexPermalink);
    assertEquals(0, indexFLname.getUsedTime());
    assertEquals(0, indexDate.getUsedTime());
    assertEquals(0, indexPermalink.getUsedTime());

    collection.find(new BasicDBObject("firstname", "firstname0"));
    // No index used.
    assertEquals(0, indexFLname.getUsedTime());
    assertEquals(0, indexDate.getUsedTime());
    assertEquals(0, indexPermalink.getUsedTime());

    List<DBObject> objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0")).toArray();
    assertEquals(2, objects.size());
    assertEquals(1, indexFLname.getUsedTime());
    assertEquals(0, indexDate.getUsedTime());
    assertEquals(0, indexPermalink.getUsedTime());

    objects = collection.find(new BasicDBObject("firstname", "firstname0").append("lastname", "lastname0").append("date", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getUsedTime());
    assertEquals(0, indexDate.getUsedTime());
    assertEquals(0, indexPermalink.getUsedTime());

    objects = collection.find(new BasicDBObject("permalink", 0)).toArray();
    assertEquals(1, objects.size());
    assertEquals(2, indexFLname.getUsedTime());
    assertEquals(0, indexDate.getUsedTime());
    assertEquals(1, indexPermalink.getUsedTime());
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
}
