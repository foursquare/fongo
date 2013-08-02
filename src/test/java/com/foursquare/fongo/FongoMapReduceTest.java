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
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
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
    System.out.println(results);
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"count\" : 2.0}}, { \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"count\" : 3.0}}]\n"), results);
//    coll.mapReduce()
  }

}
