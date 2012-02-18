package com.foursquare.fongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

public class UpdateEngineTest {

  @Test
  public void testBasicUpdate() {
    UpdateEngine updateEngine = new UpdateEngine();
    assertEquals(new BasicDBObject("_id", 1).append("a",1),
        updateEngine.doUpdate(new BasicDBObject("_id", 1).append("a", 5).append("b", 1),
            new BasicDBObject("a", 1)));
  }
  
  @Test
  public void testOnlyOneAtomicUpdatePerKey(){
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$set").append("a", 5).pop()
        .push("$inc").append("a", 3).pop().get();

    try {
      updateEngine.doUpdate(new BasicDBObject(), update);
      fail("should get exception");
    } catch (Exception e) {
    }
  }
  
  @Test
  public void testSetOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$set").append("a", 5).pop().get();
    
    assertEquals(new BasicDBObject("_id", 1).append("a",5).append("b", 1),
        updateEngine.doUpdate(new BasicDBObject("_id", 1).append("a", 1).append("b", 1), update));
  }
  
  @Test
  public void testEmbeddedSetOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$set").append("a.b", 5).pop().get();
    
    assertEquals(new BasicDBObject("a", new BasicDBObject("b", 5)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testUnSetOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$unset").append("a", 1).pop().get();
    
    assertEquals(new BasicDBObject("_id", 1).append("b", 1),
        updateEngine.doUpdate(new BasicDBObject("_id", 1).append("a", 1).append("b", 1), update));
  }
  
  @Test
  public void testEmbeddedUnSetOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$unset").append("a.b", 1).pop().get();
    
    assertEquals(new BasicDBObject("_id", 1).append("a", new BasicDBObject()),
        updateEngine.doUpdate(new BasicDBObject("_id", 1).append("a", new BasicDBObject("b",1)), update));
  }
  
  @Test
  public void testIncOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$inc").append("a", 5).pop().get();
    
    assertEquals(new BasicDBObject("a",6),
        updateEngine.doUpdate(new BasicDBObject("a", 1), update));
    assertEquals(new BasicDBObject("a", 5),
        updateEngine.doUpdate(new BasicDBObject(), update));
    assertEquals(new BasicDBObject("a", 8.1),
        updateEngine.doUpdate(new BasicDBObject("a", 3.1), update));
  }
  
  @Test
  public void testPushOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$push").append("a", 2).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1,2)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1)), update));
    assertEquals(new BasicDBObject("a", Util.list(2)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testPushAndIncOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject query = new BasicDBObject("_id", 1);
    DBObject update = new BasicDBObjectBuilder()
      .push("$push").push("n").append("_id", 2).append("u", 3).pop().pop()
      .push("$inc").append("c",4).pop().get();
    DBObject expected = new BasicDBObjectBuilder().append("_id", 1).append("n", Util.list(new BasicDBObject("_id", 2).append("u", 3))).append("c", 4).get();
    assertEquals(expected, updateEngine.doUpdate(query, update));
  }
  
  @Test
  public void testPushAllOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pushAll").append("a", Util.list(2,3)).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1,2,3)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1)), update));
    assertEquals(new BasicDBObject("a", Util.list(2,3)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testAddToSetOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$addToSet").append("a", 2).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1,2)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1)), update));
    assertEquals(new BasicDBObject("a", Util.list(1,2)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1,2)), update));
    assertEquals(new BasicDBObject("a", Util.list(2)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testAddToSetEachOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$addToSet")
        .push("a").append("$each", Util.list(2,3)).pop().pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1,2,3)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1)), update));
    assertEquals(new BasicDBObject("a", Util.list(1,2,3)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1,2)), update));
    assertEquals(new BasicDBObject("a", Util.list(2,3)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testPopLastOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pop").append("a", 1).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1,2)), update));
    assertEquals(new BasicDBObject("a", Util.list()),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1)), update));
    assertEquals(new BasicDBObject(),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  @Test
  public void testPopFirstOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pop").append("a", -1).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(2)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1,2)), update));
  }
  
  @Test
  public void testSimplePullOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pull").append("a", 1).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(2,3)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(1,2,1,3,1)), update));
  }
  
  @Test
  public void testEmbeddedPullOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pull").push("a")
        .append("b",1).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(new BasicDBObject("b",2).append("f", 1))),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(
            new BasicDBObject("b", 1).append("f", 1),
            new BasicDBObject("b", 2).append("f", 1),
            new BasicDBObject("b", 1).append("f", 1)
        )), update));
  }

  @Test
  public void testPullAllOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$pullAll").append("a", Util.list(2,3)).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(1,4)),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(2,1,2,3,4,2,3)), update));
  }
  
  @Test
  public void testBitAndOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$bit")
        .push("a").append("and", 5).pop().get();
    
    assertEquals(new BasicDBObject("a", 11 & 5),
        updateEngine.doUpdate(new BasicDBObject("a", 11), update));
  }
  
  @Test
  public void testBitOrOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$bit")
        .push("a").append("or", 5).pop().get();
    
    assertEquals(new BasicDBObject("a", 11 | 5),
        updateEngine.doUpdate(new BasicDBObject("a", 11), update));
  }
  
  @Test
  public void testBitAndOrOperation() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$bit")
        .push("a").append("and", 5).append("or", 2).pop().get();
    
    assertEquals(new BasicDBObject("a", (11 & 5) | 2),
        updateEngine.doUpdate(new BasicDBObject("a", 11), update));
  }
  
  @Test 
  public void testPositionalOperator() {
    UpdateEngine updateEngine = new UpdateEngine(true);
    DBObject update = new BasicDBObjectBuilder().push("$inc")
        .append("b.$.c",1).pop().get();
    
    assertEquals(new BasicDBObject("b", Util.list(new BasicDBObject("c", 2).append("n","jon"))),
        updateEngine.doUpdate(new BasicDBObject("b", Util.list(
            new BasicDBObject("c", 1).append("n", "jon"))), update, new BasicDBObject("b.n", "jon")));
  }
  
  @Test 
  public void testPositionalArrayOperator() {
    UpdateEngine updateEngine = new UpdateEngine(true);
    DBObject update = new BasicDBObjectBuilder().push("$inc")
        .append("b.$",1).pop().get();
    
    assertEquals(new BasicDBObject("b", Util.list(1,3,3)),
        updateEngine.doUpdate(new BasicDBObject("b", Util.list(1,2,3)), update, new BasicDBObject("b", 2)));
  }
  
  @Test 
  public void testArrayIndexOperator() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$inc")
        .append("a.1.b",1).pop().get();
    
    assertEquals(new BasicDBObject("a", Util.list(new BasicDBObject("b", 1), new BasicDBObject("b",2))),
        updateEngine.doUpdate(new BasicDBObject("a", Util.list(
            new BasicDBObject("b", 1), 
            new BasicDBObject("b",1))
        ), update));
  }
  
  @Test 
  public void testEmbeddedIntOperator() {
    UpdateEngine updateEngine = new UpdateEngine();
    DBObject update = new BasicDBObjectBuilder().push("$inc")
        .append("a.1",1).pop().get();
    
    assertEquals(new BasicDBObject("a", new BasicDBObject("1", 1)),
        updateEngine.doUpdate(new BasicDBObject(), update));
  }
  
  
}
