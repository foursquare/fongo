package com.foursquare.fongo;

import ch.qos.logback.classic.Level;
import com.foursquare.fongo.impl.ExpressionParser;
import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.FongoDBCollection;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bson.BSON;
import org.bson.Transformer;
import org.bson.types.ObjectId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class FongoMapReduceTest {


  @Rule
  public FongoRule fongoRule = new FongoRule(!true);


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
    String reduce = "function(name, values) {                           return Array.sum(values);                       };";

    MapReduceOutput output = coll.mapReduce(map, reduce, "result", new BasicDBObject("state", "MA"));

    List<DBObject> results = fongoRule.newCollection("result").find().toArray();

    assertEquals(fongoRule.parse("[{ \"_id\" : \"phil\" , \"value\" : 474.0}]"), results);
  }
}
