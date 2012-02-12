package com.foursquare.fongo;

import static org.junit.Assert.*;

import org.junit.Test;

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

}
