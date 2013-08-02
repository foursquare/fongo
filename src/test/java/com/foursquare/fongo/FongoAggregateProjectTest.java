package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class FongoAggregateProjectTest {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  public void testConcat() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $concat: [ \"$item.type\",\n" +
        "                                                    \" \",\n" +
        "                                                    \"$item.category\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry pie\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's pie\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"chicken pot pie\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  public void testConcatNullOrMissing() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } },\n" +
        "{ _id: 5, item: { sec: \"beverage\", type: \"coffee\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $concat: [ \"$item.type\",\n" +
        "                                                    \" \",\n" +
        "                                                    \"$item.category\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
        "               { \"_id\" : 2, \"food\" : \"cherry pie\" },\n" +
        "               { \"_id\" : 3, \"food\" : \"shepherd's pie\" },\n" +
        "               { \"_id\" : 4, \"food\" : \"chicken pot pie\" },\n" +
        "               { \"_id\" : 5, \"food\" : null }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  @Ignore // TODO(twillouer)
  public void testConcatNullOrMissingIfNull() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } },\n" +
        "{ _id: 5, item: { sec: \"beverage\", type: \"coffee\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $concat: [ { $ifNull: [\"$item.type\", \"<unknown type>\"] },\n" +
        "                                                    \" \",\n" +
        "                                                    { $ifNull: [\"$item.category\", \"<unknown category>\"] }\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
        "               { \"_id\" : 2, \"food\" : \"cherry pie\" },\n" +
        "               { \"_id\" : 3, \"food\" : \"shepherd's pie\" },\n" +
        "               { \"_id\" : 4, \"food\" : \"chicken pot pie\" },\n" +
        "               { \"_id\" : 5, \"food\" : \"coffee <unknown category>\" }]\n"), result);
  }

  @Test
  @Ignore // TODO(twillouer)
  public void testProjectDoenstSendArray() {
    DBCollection coll = fongoRule.newCollection();
    coll.insert(new BasicDBObject("a", Util.list(1, 2, 3)));

    // project doesn't handle array
    AggregationOutput output = coll.aggregate(new BasicDBObject("$project", new BasicDBObject("e", "$a.0")));
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(new BasicDBList(), result.get(0).get("e"));
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/strcasecmp/
   */
  @Test
  public void testStrcasecmp() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $strcasecmp: [ \"$item.type\",\n" +
        "                                                    \"$item.category\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : -1 },\n" +
        "                    { \"_id\" : 2, \"food\" : -1 },\n" +
        "                    { \"_id\" : 3, \"food\" : 1},\n" +
        "                    { \"_id\" : 4, \"food\" : -1 }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/strcasecmp/
   */
  @Test
  public void testStrcasecmpWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"APPLE\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"ANN\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $strcasecmp: [ \"$item.type\",\n" +
        "                                                    \"apple\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : 0 },\n" +
        "                    { \"_id\" : 2, \"food\" : 0 },\n" +
        "                    { \"_id\" : 3, \"food\" : 1 },\n" +
        "                    { \"_id\" : 4, \"food\" : -1 }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/strcasecmp/
   */
  @Test
  public void testStrcasecmpMustBeAFailure() {
    ExpectedMongoException.expectCommandFailure(exception, 16020);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $strcasecmp: [ \"$item.type\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    coll.aggregate(project);
  }


  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"apple\", 0, 1 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"a\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"a\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"a\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"a\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithField() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"$item.type\", 0, 1 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"a\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"c\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"s\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"c\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithFieldOutOf() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"$item.type\", 15, 18 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithFieldTooLong() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"$item.type\", 0, 18 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"chicken pot\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithNull() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\" }}]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"$item.type\", 0, 1 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"a\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"c\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"s\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"\"}]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/substr/
   */
  @Test
  public void testSubstrWithErrorParams() {
    ExpectedMongoException.expectCommandFailure(exception, 16020);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\" }}]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $substr: [ \"$item.type\", 0 ]" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    coll.aggregate(project);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/ifNull/
   */
  @Test
  public void testIfNullWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $ifNull: [ \"$item.type\",\n" +
        "                                                    \"wasNull\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"wasNull\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/ifNull/
   */
  @Test
  public void testIfNullWithField() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $ifNull: [ \"$item.type\",\n" +
        "                                                    \"$item.category\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"pie\" }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/ifNull/
   */
  @Test
  public void testIfNullWithFieldArray() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", repl : [1,2,3] } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $ifNull: [ \"$item.type\",\n" +
        "                                                    \"$item.repl\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : \"apple\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's\" },\n" +
        "                    { \"_id\" : 4, \"food\" : [1,2,3] }]\n"), result);
  }


  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/cmp/
   */
  @Test
  public void testCmp() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $cmp: [ \"$item.type\",\n" +
        "                                                    \"$item.category\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : -1 },\n" +
        "                    { \"_id\" : 2, \"food\" : -1 },\n" +
        "                    { \"_id\" : 3, \"food\" : 1},\n" +
        "                    { \"_id\" : 4, \"food\" : -1 }]\n"), result);
  }

  @Test
  public void testCmpWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $cmp: [ \"$item.type\",\n" +
        "                                                    \"apple\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[{ \"_id\" : 1, \"food\" : 0 },\n" +
        "                    { \"_id\" : 2, \"food\" : 1 },\n" +
        "                    { \"_id\" : 3, \"food\" : 1 },\n" +
        "                    { \"_id\" : 4, \"food\" : 1 }]\n"), result);
  }

  @Test
  public void testCmpMustBeAFailure() {
    ExpectedMongoException.expectCommandFailure(exception, 16020);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $cmp: [ \"$item.type\"\n" +
        "                                                  ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    coll.aggregate(project);
  }


  @Test
  public void testToUpper() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toUpper: \"$item.type\"\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 2 , \"food\" : \"CHERRY\"}," +
        " { \"_id\" : 3 , \"food\" : \"SHEPHERD'S\"}," +
        " { \"_id\" : 4 , \"food\" : \"CHICKEN POT\"}]"), result);
  }

  @Test
  public void testToUpperWithNull() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toUpper: \"$item.type\"\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 2 , \"food\" : \"CHERRY\"}," +
        " { \"_id\" : 3 , \"food\" : \"SHEPHERD'S\"}," +
        " { \"_id\" : 4 , \"food\" : \"\"}]"), result);
  }

  @Test
  public void testToUpperMustHandleArray() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toUpper: [ \"$item.type\" ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 2 , \"food\" : \"CHERRY\"}," +
        " { \"_id\" : 3 , \"food\" : \"SHEPHERD'S\"}," +
        " { \"_id\" : 4 , \"food\" : \"CHICKEN POT\"}]"), result);
  }

  @Test
  public void testToUpperWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toUpper: \"apple\" }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 2 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 3 , \"food\" : \"APPLE\"}," +
        " { \"_id\" : 4 , \"food\" : \"APPLE\"}]"), result);
  }


  @Test
  public void testToLower() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"APPLE\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"CHERRY\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken POT\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toLower: \"$item.type\"\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"apple\"}," +
        " { \"_id\" : 2 , \"food\" : \"cherry\"}," +
        " { \"_id\" : 3 , \"food\" : \"shepherd's\"}," +
        " { \"_id\" : 4 , \"food\" : \"chicken pot\"}]"), result);
  }

  @Test
  public void testToLowerWithNull() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"APPLE\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"CHERRY\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\"} }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toLower: \"$item.type\"\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"apple\"}," +
        " { \"_id\" : 2 , \"food\" : \"cherry\"}," +
        " { \"_id\" : 3 , \"food\" : \"shepherd's\"}," +
        " { \"_id\" : 4 , \"food\" : \"\"}]"), result);
  }

  @Test
  public void testToLowerMustHandleArray() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"APPLE\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"CHERRY\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"SHEPHERD'S\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"CHICKEN pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toLower: [ \"$item.type\" ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"apple\"}," +
        " { \"_id\" : 2 , \"food\" : \"cherry\"}," +
        " { \"_id\" : 3 , \"food\" : \"shepherd's\"}," +
        " { \"_id\" : 4 , \"food\" : \"chicken pot\"}]"), result);
  }

  @Test
  public void testToLowerMustNotHandleBigArray() {
    //com.mongodb.CommandFailureException: { "serverUsed" : "/127.0.0.1:27017" , "errmsg" : "exception: the $toLower operator requires 1 operand(s)" , "code" : 16020 , "ok" : 0.0}
    ExpectedMongoException.expectCommandFailure(exception, 16020);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"APPLE\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"CHERRY\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"SHEPHERD'S\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"CHICKEN pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toLower: [ \"$item.type\", \"$item.category\" ]\n" +
        "                                       }\n" +
        "                                }\n" +
        "                   }");

    coll.aggregate(project);
  }

  @Test
  public void testToLowerWithValue() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $project: { food:\n" +
        "                                       { $toLower: \"APPLE\" }\n" +
        "                                }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(fongoRule.parse("[ { \"_id\" : 1 , \"food\" : \"apple\"}," +
        " { \"_id\" : 2 , \"food\" : \"apple\"}," +
        " { \"_id\" : 3 , \"food\" : \"apple\"}," +
        " { \"_id\" : 4 , \"food\" : \"apple\"}]"), result);
  }

  @Test
  public void shouldHandleProjectWithRename() {
    DBObject project = new BasicDBObject("$project", new BasicDBObject("renamedDate", "$date"));
    AggregationOutput output = createTestCollection().aggregate(project);

    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    BasicDBList result = (BasicDBList) output.getCommandResult().get("result");
    assertEquals(10, result.size());
    assertTrue(((DBObject) result.get(0)).containsField("renamedDate"));
    assertTrue(((DBObject) result.get(0)).containsField("_id"));
  }

  @Test
  public void shouldHandleProjectWithSublist() {
    DBCollection collection = createTestCollection();
    collection.insert(new BasicDBObject("myId", "sublist").append("sub", new BasicDBObject("obj", new BasicDBObject("ect", 1))));

    DBObject matching = new BasicDBObject("$match", new BasicDBObject("myId", "sublist"));
    DBObject project = new BasicDBObject("$project", new BasicDBObject("bar", "$sub.obj.ect"));
    AggregationOutput output = collection.aggregate(matching, project);

    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    BasicDBList result = (BasicDBList) output.getCommandResult().get("result");
    assertEquals(1, result.size());
    assertTrue(((DBObject) result.get(0)).containsField("bar"));
    assertEquals(1, ((DBObject) result.get(0)).get("bar"));
  }

  @Test
  public void shouldHandleProjectWithTwoSublist() {
    DBCollection collection = createTestCollection();
    collection.insert(new BasicDBObject("myId", "sublist").append("sub", new BasicDBObject("obj", new BasicDBObject("ect", 1).append("ect2", 2))));

    DBObject matching = new BasicDBObject("$match", new BasicDBObject("myId", "sublist"));
    DBObject project = new BasicDBObject("$project", new BasicDBObject("bar", "$sub.obj.ect").append("foo", "$sub.obj.ect2"));
    AggregationOutput output = collection.aggregate(matching, project);

    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    BasicDBList result = (BasicDBList) output.getCommandResult().get("result");
    assertEquals(1, result.size());
    assertTrue(((DBObject) result.get(0)).containsField("bar"));
    assertEquals(1, ((DBObject) result.get(0)).get("bar"));
    assertTrue(((DBObject) result.get(0)).containsField("foo"));
    assertEquals(2, ((DBObject) result.get(0)).get("foo"));
    assertTrue(!((DBObject) result.get(0)).containsField("sub"));
  }

  @Test
  public void shouldRenameIntoAnObject() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("name", "jon").append("lastname", "hoff").append("_id", 1));
    collection.insert(new BasicDBObject("name", "will").append("lastname", "del").append("_id", 2));

    DBObject project = new BasicDBObject("$project", new BasicDBObject("author", new BasicDBObject("name", "$name").append("lastname", "$lastname")));
    AggregationOutput output = collection.aggregate(project);

    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    BasicDBList result = (BasicDBList) output.getCommandResult().get("result");
    assertEquals(Util.list(new BasicDBObject("_id", 1).append("author", new BasicDBObject("name", "jon").append("lastname", "hoff")),
        new BasicDBObject("_id", 2).append("author", new BasicDBObject("name", "will").append("lastname", "del"))), result);
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
