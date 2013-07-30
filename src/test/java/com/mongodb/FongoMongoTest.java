package com.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.foursquare.fongo.Fongo;
import org.junit.Test;

public class FongoMongoTest {

  @Test
  public void testIsMongosConnection() {
    Mongo mongo = new Fongo("test").getMongo();
    assertFalse("should be mocked", mongo.isMongosConnection());
  }

  @Test
  public void mongoClientHasOptions() {
    MongoClient mongoClient = new Fongo("test").getMongo();
    assertNotNull(mongoClient.getMongoClientOptions());
    assertNotNull(mongoClient.getMongoOptions());
  }
  
  @Test
  public void mongoHasWriteConcern() {
    Fongo fongo = new Fongo("test");
    assertEquals(WriteConcern.ACKNOWLEDGED, fongo.getMongo().getWriteConcern());
    assertEquals(WriteConcern.ACKNOWLEDGED, fongo.getWriteConcern());
  }
}
