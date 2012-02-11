package com.foursquare.fongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
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
    assertEquals(Arrays.<DBObject>asList(newMe(), newMe().append("extradata", 1)), results);
  }
  
  @Test
  public void testBasicOperators() {
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
  
  @Test
  public void testAllOperator(){
    DBObject query = new BasicDBObjectBuilder().push("a").add("$all", Arrays.asList(2,3)).pop().get();
    List<DBObject> results = doFilter(
        query,
        new BasicDBObject("a", Arrays.asList(2,3)),
        new BasicDBObject("a", Arrays.asList(1,3,4)),
        new BasicDBObject("a", Arrays.asList(1,2,3)),
        new BasicDBObject("a", Arrays.asList(1,3,4))
    );
    assertEquals(Arrays.<DBObject>asList(
        new BasicDBObject("a", Arrays.asList(2,3)),
        new BasicDBObject("a", Arrays.asList(1,2,3))
    ), results);
  }
  
  @Test
  public void tesExistsOperator(){
    DBObject query = new BasicDBObjectBuilder().push("a").add("$exists", true).pop().get();
    List<DBObject> results = doFilter(
        query,
        new BasicDBObject("a", null),
        new BasicDBObject("b", null),
        new BasicDBObject("a", "hi"),
        new BasicDBObject("b", "hi")
    );
    assertEquals(Arrays.<DBObject>asList(
        new BasicDBObject("a", null),
        new BasicDBObject("a", "hi")
    ), results);
  }
  
  @Test
  public void testModOperator(){
    fail("implement me");
  }
  
  @Test
  public void testInOperator(){
    fail("implement me");
  }
  
  @Test
  public void testNorOperator(){
    fail("implement me");
  }
  
  @Test
  public void testOrOperator(){
    fail("implement me");
  }
  
  @Test
  public void testSizeOperator(){
    fail("implement me");
  }
  
  @Test
  public void testNotOperator(){
    fail("implement me");
  }
  
  @Test
  public void testEmbeddedMatch(){
    fail("implement me");
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
    assertEquals(expected, results);
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
