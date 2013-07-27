package com.foursquare.fongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class FongoIndexTest {

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
}
