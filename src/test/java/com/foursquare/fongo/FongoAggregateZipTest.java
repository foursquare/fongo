package com.foursquare.fongo;

import com.foursquare.fongo.impl.Util;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class FongoAggregateZipTest {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  private DBCollection collection;

  @Before
  public void setup() throws Exception {
    collection = fongoRule.insertFile(fongoRule.newCollection(), "/zips.json");
  }

  // see http://stackoverflow.com/questions/11418985/mongodb-aggregation-framework-group-over-multiple-values
  @Test
  public void shouldHandleStatesWithPopulationsOver5Million() {
    List<DBObject> pipeline = fongoRule.parseList("[{ $group :\n" +
        "                         { _id : \"$state\",\n" +
        "                           totalPop : { $sum : \"$pop\" } } },\n" +
        "{ $match : {totalPop : { $gte : 5000000 } } },\n" +
        "{ $sort : {_id:1}}\n" +
        "]");

    // Aggregate
    AggregationOutput output = collection.aggregate(pipeline.get(0), pipeline.get(1), pipeline.get(2));
    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));


    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    Assert.assertEquals(fongoRule.parseDEObject("[ { \"_id\" : \"MA\" , \"totalPop\" : 6016425} , " +
        "{ \"_id\" : \"NJ\" , \"totalPop\" : 7730188} , " +
        "{ \"_id\" : \"NY\" , \"totalPop\" : 12950936}]"), resultAggregate);
  }

  @Test
  public void shouldHandleLargestAndSmallestCitiesByState() {
    List<DBObject> pipeline = fongoRule.parseList("[   { $group:\n" +
        "      { _id: { state: \"$state\", city: \"$city\" },\n" +
        "        pop: { $sum: \"$pop\" } } },\n" +
        "    { $sort: { pop: 1 } },\n" +
        "    { $group:\n" +
        "      { _id : \"$_id.state\",\n" +
        "        biggestCity:  { $last: \"$_id.city\" },\n" +
        "        biggestPop:   { $last: \"$pop\" },\n" +
        "        smallestCity: { $first: \"$_id.city\" },\n" +
        "        smallestPop:  { $first: \"$pop\" } } },\n" +
        "    { $project:\n" +
        "      { _id: 0,\n" +
        "        state: \"$_id\",\n" +
        "        biggestCity:  { name: \"$biggestCity\",  pop: \"$biggestPop\" },\n" +
        "        smallestCity: { name: \"$smallestCity\", pop: \"$smallestPop\" } } },\n" +
        "    { $sort : { \"biggestCity.name\" : 1 } }\n" +
        "]");


    // Aggregate
    AggregationOutput output = collection.aggregate(pipeline.get(0), pipeline.get(1), pipeline.get(2), pipeline.get(3), pipeline.get(4));

    // Assert
    assertTrue(output.getCommandResult().ok());
    assertTrue(output.getCommandResult().containsField("result"));

    List<DBObject> resultAggregate = (List<DBObject>) output.getCommandResult().get("result");
    // TODO(twillouer) : $project { _id : 0 } must remove the id field but $sort add him...
//    Assert.assertEquals(fongoRule.parseList("" +
//        "[ { \"biggestCity\" : { \"name\" : \"BRIDGEPORT\" , \"pop\" : 141638} , \"smallestCity\" : { \"name\" : \"EAST KILLINGLY\" , \"pop\" : 25} , \"state\" : \"CT\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"BROOKLYN\" , \"pop\" : 2300504} , \"smallestCity\" : { \"name\" : \"NEW HYDE PARK\" , \"pop\" : 1} , \"state\" : \"NY\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"BURLINGTON\" , \"pop\" : 39127} , \"smallestCity\" : { \"name\" : \"UNIV OF VERMONT\" , \"pop\" : 0} , \"state\" : \"VT\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"CRANSTON\" , \"pop\" : 176404} , \"smallestCity\" : { \"name\" : \"CLAYVILLE\" , \"pop\" : 45} , \"state\" : \"RI\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"MANCHESTER\" , \"pop\" : 106452} , \"smallestCity\" : { \"name\" : \"WEST NOTTINGHAM\" , \"pop\" : 27} , \"state\" : \"NH\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"NEWARK\" , \"pop\" : 275572} , \"smallestCity\" : { \"name\" : \"IMLAYSTOWN\" , \"pop\" : 17} , \"state\" : \"NJ\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"PORTLAND\" , \"pop\" : 63268} , \"smallestCity\" : { \"name\" : \"BUSTINS ISLAND\" , \"pop\" : 0} , \"state\" : \"ME\"} , " +
//        "{ \"biggestCity\" : { \"name\" : \"WORCESTER\" , \"pop\" : 169856} , \"smallestCity\" : { \"name\" : \"BUCKLAND\" , \"pop\" : 16} , \"state\" : \"MA\"}]"), resultAggregate);
    assertEquals(8, resultAggregate.size());
    assertEquals("BRIDGEPORT", Util.extractField(resultAggregate.get(0), "biggestCity.name"));
    assertEquals(141638, Util.extractField(resultAggregate.get(0), "biggestCity.pop"));
    assertEquals("EAST KILLINGLY", Util.extractField(resultAggregate.get(0), "smallestCity.name"));
    assertEquals(25, Util.extractField(resultAggregate.get(0), "smallestCity.pop"));
    DBObject last = resultAggregate.get(resultAggregate.size() - 1);
    assertEquals("WORCESTER", Util.extractField(last, "biggestCity.name"));
    assertEquals(169856, Util.extractField(last, "biggestCity.pop"));
    assertEquals("BUCKLAND", Util.extractField(last, "smallestCity.name"));
    assertEquals(16, Util.extractField(last, "smallestCity.pop"));
  }
}