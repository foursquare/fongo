package com.mongodb;

import static org.junit.Assert.assertFalse;

import com.foursquare.fongo.Fongo;
import org.junit.Test;

public class FongoMongoTest {

  @Test
  public void testIsMongosConnection() {
    Mongo mongo = new Fongo("test").getMongo();
    assertFalse("should be mocked", mongo.isMongosConnection());
  }

}
