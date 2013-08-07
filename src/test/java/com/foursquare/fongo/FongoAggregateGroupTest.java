package com.foursquare.fongo;

import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class FongoAggregateGroupTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  @Ignore("@twillouer")
  public void testConcat() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDEObject("{ $group: { _id:\n" +
        "                                    { $concat: [ \"$item.sec\",\n" +
        "                                                 \": \",\n" +
        "                                                 \"$item.category\"\n" +
        "                                               ]\n" +
        "                                    },\n" +
        "                               count: { $sum: 1 }\n" +
        "                             }\n" +
        "                   }");

    AggregationOutput output = coll.aggregate(project);
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    System.out.println(result);
    assertNotNull(result);
    assertEquals(JSON.parse("[\n" +
        "               { \"_id\" : \"main: pie\", \"count\" : 2 },\n" +
        "               { \"_id\" : \"dessert: pie\", \"count\" : 2 }\n" +
        "             ]"), result);
  }
}
