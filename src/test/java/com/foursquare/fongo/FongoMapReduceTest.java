package com.foursquare.fongo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FongoMapReduceTest {
  private static final Logger LOG = LoggerFactory.getLogger(FongoMapReduceTest.class);

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);


  // see http://no-fucking-idea.com/blog/2012/04/01/using-map-reduce-with-mongodb/
  @Test
  public void testMapReduceSimple() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{url: \"www.google.com\", date: 1, trash_data: 5 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 1, trash_data: 13 },\n" +
        " {url: \"www.google.com\", date: 1, trash_data: 1 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 69 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 256 }]");


    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());


    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"count\" : 2.0}}, { \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"count\" : 3.0}}]"), results);
  }


  @Test
  public void testZipMapReduce() throws IOException {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertFile(coll, "/zips.json");

    String map = "function () {\n" +
        "    var pitt = [-80.064879, 40.612044];\n" +
        "    var phil = [-74.978052, 40.089738];\n" +
        "\n" +
        "    function distance(a, b) {\n" +
        "        var dx = a[0] - b[0];\n" +
        "        var dy = a[1] - b[1];\n" +
        "        return Math.sqrt(dx * dx + dy * dy);\n" +
        "    }\n" +
        "\n" +
        "    if (distance(this.loc, pitt) < distance(this.loc, phil)) {\n" +
        "        emit(\"pitt\",1);\n" +
        "    } else {\n" +
        "        emit(\"phil\",1);\n" +
        "    }\n" +
        "}\n";
    String reduce = "function(name, values) { return Array.sum(values); };";

    MapReduceOutput output = coll.mapReduce(map, reduce, "result", new BasicDBObject("state", "MA"));

    List<DBObject> results = fongoRule.newCollection("result").find().toArray();

    assertEquals(fongoRule.parse("[{ \"_id\" : \"phil\" , \"value\" : 474.0}]"), results);
  }

  @Test
  public void testMapReduceMapInError() {
    ExpectedMongoException.expectCommandFailure(exception, 16722);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{url: \"www.google.com\", date: 1, trash_data: 5 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 1, trash_data: 13 },\n" +
        " {url: \"www.google.com\", date: 1, trash_data: 1 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 69 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 256 }]");


    String map = "function(){    ;";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());
  }

  @Test
  public void testMapReduceReduceInError() {
    ExpectedMongoException.expectCommandFailure(exception, 16722);
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{url: \"www.google.com\", date: 1, trash_data: 5 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 1, trash_data: 13 },\n" +
        " {url: \"www.google.com\", date: 1, trash_data: 1 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 69 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 256 }]");


    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());
  }

}
