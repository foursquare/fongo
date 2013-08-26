package com.mongodb;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.foursquare.fongo.Fongo;

public class FongoDBCollectionTest {
  private FongoDBCollection collection;

  @Before
  public void setUp() {
    collection = (FongoDBCollection) new Fongo("test").getDB("test").getCollection("test");
  }

  @Test
  public void applyProjectionsInclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 1));
    DBObject expected = new BasicDBObject().append("_id", "_id").append("b", "b");

    assertEquals("applied", expected, actual);
  }
  
  /** Tests multiprojections that are nested with the same prefix: a.b.c and a.b.d */
  @Test
  public void applyNestedProjectionsInclusionsOnly() {
      final DBObject obj = new BasicDBObjectBuilder()
          .add("_id", "_id")
          .add("foo", 123)
          .push("a")
              .push("b")
                  .append("c", 50)
                  .append("d", 1000)
                  .append("bar", 1000)
      .get();
    
    final DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("a.b.c", 1)
                                                                                 .append("a.b.d", 1));
    final DBObject expected =  new BasicDBObjectBuilder()
                        .add("_id", "_id")
                        .push("a")
                            .push("b")
                                .append("c", 50)
                                .append("d", 1000)
                           .get();
            
    assertEquals("applied", expected, actual);
  }
  

  @Test
  public void applyProjectionsInclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 1));
    BasicDBObject expected = new BasicDBObject().append("b", "b");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("_id", "_id").append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void applyProjectionsInclusionsAndExclusionsMixedThrowsException() {
    BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
    collection.applyProjections(obj, new BasicDBObject().append("a", 1).append("b", 0));
  }


  @Test
  public void applyNestedArrayFieldProjection() {
    BasicDBObject obj = new BasicDBObject("_id", 1).append("name","foo")
      .append("seq", Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject("c", "b")));
    collection.insert(obj);

    List<DBObject> results = collection.find(new BasicDBObject("_id", 1), 
        new BasicDBObject("_id", -1).append("seq.a", 1)).toArray();

    BasicDBObject expectedResult = new BasicDBObject("seq", 
      Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject()));

    assertEquals("should have projected result", Arrays.asList(expectedResult), results);
  }
  
  @Test
  public void applyNestedFieldProjection() {
    
    collection.insert(new BasicDBObject("_id", 1)
      .append("a",new BasicDBObject("b", new BasicDBObject("c", 1))));
    
    collection.insert(new BasicDBObject("_id", 2)
      .append("a",new BasicDBObject("b", 1)));
    
    collection.insert(new BasicDBObject("_id", 3));

    List<DBObject> results = collection.find(new BasicDBObject(), 
        new BasicDBObject("_id", -1).append("a.b.c", 1)).toArray();

    assertEquals("should have projected result", Arrays.asList(
      new BasicDBObject("a",new BasicDBObject("b", new BasicDBObject("c", 1))),
      new BasicDBObject("a",new BasicDBObject()),
      new BasicDBObject()
    ), results);
  }

  @Test
  public void findByListInQuery(){
    BasicDBObject existing = new BasicDBObject().append("_id", 1).append("aList", asDbList("a", "b", "c"));
    collection.insert(existing);
    DBObject result = collection.findOne(existing);
    assertEquals("should have projected result", existing, result);
  }

  BasicDBList asDbList(Object ... objects) {
     BasicDBList list = new BasicDBList();
     list.addAll(Arrays.asList(objects));
     return list;
  }

}
