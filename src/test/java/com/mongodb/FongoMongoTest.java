package com.mongodb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.net.UnknownHostException;

import com.foursquare.fongo.Fongo;
import org.junit.Test;

public class FongoMongoTest {

  @Test
  public void testIsMongosConnection() {
    Mongo mongo = new Fongo("test").getMongo();
    assertFalse("should be mocked", mongo.isMongosConnection());
  }

  @Test
  public void mongoClientHasOptions() throws UnknownHostException {
    MongoClient mongoClient = new Fongo("test").getMongo();
    assertNotNull(mongoClient.getMongoClientOptions());
    assertNotNull(mongoClient.getMongoOptions());
  }
}
