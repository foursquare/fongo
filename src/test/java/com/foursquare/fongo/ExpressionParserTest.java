package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class ExpressionParserTest {

  @Test
  public void testSimpleAndFilter() {
    List<DBObject> results = doFilter(
        newMe(), // the example to filter
        newMe(),
        new BasicDBObject("n","jon").append("a", "29"),
        new BasicDBObject("n","notjon").append("a", "31"),
        new BasicDBObject("n","jon").append("a", "31"),
        newMe().append("extradata", 1),
        new BasicDBObject("n","notjon").append("a", "29")
    );
    Assert.assertEquals(Arrays.<DBObject>asList(newMe(), newMe().append("extradata", 1)), results);
  }
  
  @Test
  public void testConditionals() {
    assertQuery(new BasicDBObject("a", new BasicDBObject("$gte", 4)), Arrays.<DBObject>asList(
        new BasicDBObject("n","stu").append("a", 4),
        new BasicDBObject("n","tim").append("a", 5)
    ));
    assertQuery(new BasicDBObject("a", new BasicDBObject("$lte", 3)), Arrays.<DBObject>asList(
        new BasicDBObject("n","neil").append("a", 1),
        new BasicDBObject("n","fred").append("a", 2),
        new BasicDBObject("n","ted").append("a", 3)
    ));
    assertQuery(new BasicDBObject("a", new BasicDBObject("$gt", 4)), Arrays.<DBObject>asList(
        new BasicDBObject("n","tim").append("a", 5)
    ));
    assertQuery(new BasicDBObject("a", new BasicDBObject("$lt", 3)), Arrays.<DBObject>asList(
        new BasicDBObject("n","neil").append("a", 1),
        new BasicDBObject("n","fred").append("a", 2)
    ));
    assertQuery(new BasicDBObject("a", new BasicDBObject("$gt", 3).append("$lt", 5)), Arrays.<DBObject>asList(
        new BasicDBObject("n","stu").append("a", 4)
    ));
    assertQuery(new BasicDBObject("a", new BasicDBObject("$ne", 3)), Arrays.<DBObject>asList(
        new BasicDBObject("n","neil").append("a", 1),
        new BasicDBObject("n","fred").append("a", 2),
        new BasicDBObject("n","stu").append("a", 4),
        new BasicDBObject("n","tim").append("a", 5)
    ));
  }

  private void assertQuery(BasicDBObject query, List<DBObject> expected) {
    List<DBObject> results = doFilter(
        query,
        new BasicDBObject("n","neil").append("a", 1),
        new BasicDBObject("n","fred").append("a", 2),
        new BasicDBObject("n","ted").append("a", 3),
        new BasicDBObject("n","stu").append("a", 4),
        new BasicDBObject("n","tim").append("a", 5)
    );
    Assert.assertEquals(expected, results);
  }

  private BasicDBObject newMe() {
    return new BasicDBObject("n","jon").append("a", 31);
  }
  
  public List<DBObject> doFilter(DBObject ref, BasicDBObject ... input) {
    ExpressionParser ep = new ExpressionParser();
    Filter filter = ep.buildFilter(ref);
    List<DBObject> results = new ArrayList<DBObject>();
    for (DBObject dbo : input) {
      if (filter.apply(dbo)) {
        results.add(dbo);
      }
    }
    return results;
  }

}
