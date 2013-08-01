package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class FongoAggregateProjectTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

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
    assertEquals(JSON.parse("[{ \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
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
    assertEquals(JSON.parse("[{ \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
        "               { \"_id\" : 2, \"food\" : \"cherry pie\" },\n" +
        "               { \"_id\" : 3, \"food\" : \"shepherd's pie\" },\n" +
        "               { \"_id\" : 4, \"food\" : \"chicken pot pie\" },\n" +
        "               { \"_id\" : 5, \"food\" : null }]\n"), result);
  }

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
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
    System.out.println(result);
    assertNotNull(result);
    assertEquals(JSON.parse("[ { \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
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
    assertEquals(JSON.parse("[{ \"_id\" : 1, \"food\" : \"apple pie\" },\n" +
        "                    { \"_id\" : 2, \"food\" : \"cherry pie\" },\n" +
        "                    { \"_id\" : 3, \"food\" : \"shepherd's pie\" },\n" +
        "                    { \"_id\" : 4, \"food\" : \"chicken pot pie\" }]\n"), result);
  }
}
