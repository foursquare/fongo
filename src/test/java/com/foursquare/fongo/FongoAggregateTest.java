package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.MyAsserts;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

// TODO : sum of double value ($sum : 1.3)
// sum of "1" (String) must return 0.

// Handle $group { _id = 0}
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

    // Aggregate
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

    // Aggregate
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

  @Test
  public void shouldUnwindList() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("author", "william").append("tags", Util.list("scala", "java", "mongo")));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    AggregationOutput output = collection.aggregate(matching, unwind);

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertEquals(3, result.size());

    // TODO : remove comment when _id can NOT be unique anymore.
//    Assert.assertEquals(fongoRule.parseList("[ { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"scala\"} ," +
//        " { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"java\"} ," +
//        " { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"mongo\"}]"), result);
    assertEquals("william", Util.extractField(result.get(0), "author"));
    assertEquals("scala", Util.extractField(result.get(0), "tags"));
    assertEquals("william", Util.extractField(result.get(1), "author"));
    assertEquals("java", Util.extractField(result.get(1), "tags"));
    assertEquals("william", Util.extractField(result.get(2), "author"));
    assertEquals("mongo", Util.extractField(result.get(2), "tags"));
  }

  @Test
  public void shouldUnwindEmptyList() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("author", "william").append("tags", Util.list()));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject project = new BasicDBObject("$project", new BasicDBObject("author", 1).append("tags", 1));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    // Aggregate
    AggregationOutput output = collection.aggregate(matching, project, unwind);

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertEquals(0, result.size());
  }

  @Test
  public void shouldHandleAvgOfField() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("date", new BasicDBObject("$avg", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group);
    Number result = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date"));
      }
    }

    assertTrue("must be a Double but was a " + result.getClass(), result instanceof Double);
    Assert.assertEquals(3.5D, result.doubleValue(), 0.00001);
  }

  @Test
  public void shouldHandleAvgOfDoubleField() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("myId", "p4").append("date", 1D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 2D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 3D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 4D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 5D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 6D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 7D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 10D));

    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", "p4"));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("date", new BasicDBObject("$avg", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group);
    Number result = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date"));
      }
    }

    assertTrue("must be a Double but was a " + result.getClass(), result instanceof Double);
    Assert.assertEquals(4.75D, result.doubleValue(), 0.000001);
  }


  @Test
  public void shouldHandleSumOfField() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject groupFields = new BasicDBObject("_id", null);
    groupFields.put("date", new BasicDBObject("$sum", "$date"));
    DBObject group = new BasicDBObject("$group", groupFields);

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group);
    Number result = -1;
    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
      DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = ((Number) resultAggregate.get("date"));
      }
    }

    assertEquals(21, result);
  }

  // Group with "simple _id"
  @Test
  public void shouldHandleSumOfNumber() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("sum", new BasicDBObject("$sum", 2)));

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group);

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    Number result = -1;
    DBObject resultAggregate = (DBObject) ((DBObject) output.getCommandResult().get("result")).get("0");
    if (resultAggregate != null && resultAggregate.containsField("sum")) {
      result = ((Number) resultAggregate.get("sum"));
    }

    Assert.assertEquals(14.0, result);
  }

  // Group with "simple _id"
  @Test
  public void shouldHandleSumOfFieldGroupedByMyId() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group, sort);

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    assertEquals(2, resultAggregate.size());
    Assert.assertEquals(Arrays.asList(
        new BasicDBObject("_id", "p0").append("count", 15),
        new BasicDBObject("_id", "p1").append("count", 6)), resultAggregate);
  }

  @Test
  public void shouldHandleSumOfValueGroupedByMyId() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", 2)));

    // Aggregate
    AggregationOutput output = collection.aggregate(match, group, sort);

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    Assert.assertEquals(Arrays.asList(
        new BasicDBObject("_id", "p0").append("count", 12.0),
        new BasicDBObject("_id", "p1").append("count", 2.0)), resultAggregate);
  }

  // see http://stackoverflow.com/questions/8161444/mongodb-getting-list-of-values-by-using-group
  @Test
  public void mustHandlePushInGroup() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n3\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n4\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]");

    DBObject group = fongoRule.parseDEObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)));

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    MyAsserts.assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n3\" , \"n4\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n2\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandlePushInGroupWithSameValue() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]\n");

    DBObject group = fongoRule.parseDEObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)));

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    MyAsserts.assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n1\" , \"n2\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n5\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandleAddToSetInGroup() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n3\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n4\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]");

    DBObject group = fongoRule.parseDEObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)));

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    MyAsserts.assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n3\" , \"n4\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n2\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandleAddToSetInGroupWithSameValue() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]");

    DBObject group = fongoRule.parseDEObject("{$group: { '_id': '$a_id', 'name': { $addToSet: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)));

    System.out.println(output);
    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");

    // Take care, order is not same on MongoDB (n2,n1)
    MyAsserts.assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n2\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n5\"]}]"), result);
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
