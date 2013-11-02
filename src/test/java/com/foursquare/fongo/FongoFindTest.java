package com.foursquare.fongo;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class FongoFindTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(!true);
  
  @Test
  public void testFindByNotExactType() {
    // Given
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("field", 12L));
    
    // When
    DBObject result = collection.findOne(new BasicDBObject("field", 12));
    
    // Then
    assertEquals(new BasicDBObject("_id", 1).append("field", 12L), result);
  }

}
