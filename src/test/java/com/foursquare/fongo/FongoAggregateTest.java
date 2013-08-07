package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.Iterator;
import java.util.List;
import junit.framework.Assert;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class FongoAggregateTest {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  @Test
  public void shouldHandleUnknownPipeline() {
    ExpectedMongoException.expect(exception, MongoException.class);
    ExpectedMongoException.expectCode(exception, 16436);
    DBObject badsort = new BasicDBObject("_id", 1);

    fongoRule.newCollection().aggregate(badsort);
    // Not found : com.mongodb.CommandFailureException: { "serverUsed" : "localhost/127.0.0.1:27017" , "errmsg" : "exception: Unrecognized pipeline stage name: '_id'" , "code" : 16436 , "ok" : 0.0}
  }

  @Test
  public void shouldGenerateErrorOnNonList() {
    ExpectedMongoException.expect(exception, MongoException.class);
    ExpectedMongoException.expectCode(exception, 15978);
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("author", "william").append("tags", "value"));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    collection.aggregate(matching, unwind);
    //    assert(output.getMessage === "exception: $unwind:  value at end of field path must be an array")
  }


  @Test
  public void shouldHandleLast() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p2", "p3"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$last", "$date")));
    AggregationOutput output = collection.aggregate(match, sort, group);
    int result = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(7, result);
  }

  @Test
  public void shoulHandleLastNullValue() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p4"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$last", "$date")));
    AggregationOutput output = collection.aggregate(match, group);
    boolean result = false;

    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        assertNull(resultAggregate.get("date"));
        result = true;
      }
    }
    assertTrue("Result not found", result);
  }

  @Test
  public void shouldHandleFirst() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p0"))));
//    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$first", "$date")));
    AggregationOutput output = collection.aggregate(match, group);
    int result = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(1, result);
  }

  @Test
  public void shoulHandleFirstNullValue() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p4"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$first", "$date")));
    AggregationOutput output = collection.aggregate(match, group);
    boolean result = false;

    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        assertNull(resultAggregate.get("date"));
        result = true;
      }
    }
    assertTrue("Result not found", result);
  }


  @Test
  public void shouldHandleMin() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$min", "$date")));
    AggregationOutput output = collection.aggregate(match, group);
    int result = 0;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(1, result);
  }

  @Test
  public void shouldHandleMax() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$max", "$date")));
    AggregationOutput output = collection.aggregate(match, group);
    int result = 0;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(6, result);
  }

  @Test
  public void shouldHandleMaxWithLimit() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject limit = new BasicDBObject("$limit", 3);
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$max", "$date")));
    AggregationOutput output = collection.aggregate(match, limit, group);
    int result = 0;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(3, result);
  }

  @Test
  public void shouldHandleMinWithSkip() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject skip = new BasicDBObject("$skip", 3);
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$min", "$date")));
    AggregationOutput output = collection.aggregate(match, skip, group);
    int result = 0;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date")).intValue();
      }
    }

    assertEquals(4, result);
  }

  @Test
  public void shouldHandleSort() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p2", "p3"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    AggregationOutput output = collection.aggregate(match, sort);
    int lastDate = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      Iterator<DBObject> it = ((java.util.List<DBObject>) (output.getCommandResult().get("result"))).iterator();
      while (it.hasNext()) {
        int date = ((Number) it.next().get("date")).intValue();
        assertTrue(lastDate < date);
        lastDate = date;
      }
    }

    Assert.assertEquals(7, lastDate);
  }

  private DBCollection createTestCollection() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("myId", "p0").append("date", 1));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 2));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 3));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 4));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 5));
    collection.insert(new BasicDBObject("myId", "p1").append("date", 6));
    collection.insert(new BasicDBObject("myId", "p2").append("date", 7));
    collection.insert(new BasicDBObject("myId", "p3").append("date", 0));
    collection.insert(new BasicDBObject("myId", "p0"));
    collection.insert(new BasicDBObject("myId", "p4"));
    return collection;
  }
}
