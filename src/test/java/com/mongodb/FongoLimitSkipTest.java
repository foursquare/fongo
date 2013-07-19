package com.mongodb;

import com.foursquare.fongo.Fongo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project XXX
 * <p/>
 * Date: 7/19/13 Time: 10:25 AM
 *
 * @author a554998
 */
public class FongoLimitSkipTest {

  private static final transient Logger LOG = LoggerFactory.getLogger(FongoLimitSkipTest.class);

  private Fongo fongo;
  private DB db;
  private DBCollection coll;

  @Before
  public void setup() {

    fongo = new Fongo("mongo server testLimitSkip");

    db = fongo.getDB("mydb");
    coll = db.getCollection("mycollection");

    // Insert 9 objects
    for (int i = 9; i > 0; i--) {
      DBObject dbo = (new BasicTestObject("" + i, i, "index=" + i)).asDBObject();
      coll.insert(dbo);
    }

  }

  @After
  public void teardown() {
    db.dropDatabase();
  }

  @Test
  public void testLimit() {

    LOG.info("Test find with limit");
    // Show & count objects in db
    DBCursor cursor = coll.find();
    int count = scanCursor(cursor);
    assert (count == 9);

    // Set a cursor limit of 8 & check that 8 entries are returned
    cursor = coll.find().limit(8);
    count = scanCursor(cursor);
    assert (count == 8);

    // Now set the limit to 3 & check 3 entries are returned
    cursor = coll.find().limit(3); // Set cursor limit to 3
    count = scanCursor(cursor);
    assert (count == 3);

    // Repeat last test & check the expected entries are returned
    cursor = coll.find().limit(3);
    assert (9 == (Integer) cursor.next().get("intValue"));
    assert (8 == (Integer) cursor.next().get("intValue"));
    assert (7 == (Integer) cursor.next().get("intValue"));

  }

  @Test
  public void testSkip() {

    LOG.info("Test find with skip");
    // Show & count objects in db
    DBCursor cursor = coll.find();
    int count = scanCursor(cursor);
    assert (count == 9);

    // Set skip=8 & check that 1 entry is returned
    cursor = coll.find().skip(8);
    count = scanCursor(cursor);
    assert (count == 1);

    // Now set skip=7 & check 2 entries are returned
    cursor = coll.find().skip(7); // Set cursor limit to 3
    count = scanCursor(cursor);
    assert (count == 2);

    // Repeat last test & check the expected entries are returned
    cursor = coll.find().skip(7);
    assert (2 == (Integer) cursor.next().get("intValue"));
    assert (1 == (Integer) cursor.next().get("intValue"));

  }

  @Test
  public void testLimitAndSkip() {

    LOG.info("Test find with limit and skip");
    // Show & count objects in db
    DBCursor cursor = coll.find();
    int count = scanCursor(cursor);
    assert (count == 9);

    // Set limit=3, skip=4 & check that 3 entries is returned
    cursor = coll.find().limit(3).skip(4);
    count = scanCursor(cursor);
    assert (count == 3);

    // Now set limit=3, skip=7 & check 2 entries are returned
    cursor = coll.find().limit(3).skip(7); // Set cursor limit to 3
    count = scanCursor(cursor);
    assert (count == 2);

    // Repeat last test & check the expected entries are returned
    cursor = coll.find().limit(3).skip(7);
    assert (2 == (Integer) cursor.next().get("intValue"));
    assert (1 == (Integer) cursor.next().get("intValue"));

    // Set limit 3, skip 10 - nothing should be returned
    cursor = coll.find().limit(3).skip(10);
    LOG.debug("cursor = {}", cursor);
    count = scanCursor(cursor);
    assert (count == 0);

  }

  private int scanCursor(DBCursor cursor) {
    // We count here becuase we can't use the DBCursor count() method
    int i = 0;
    while (cursor.hasNext()) {
      i++;
      if (LOG.isDebugEnabled()) {
        LOG.debug(cursor.next().toString());
      } else {
        cursor.next();
      }
    }
    LOG.debug("Cursor contains {} entries", i);
    return i;
  }

}

/**
 * Basic object for test purposes.
 * Date: 7/10/13 Time: 3:13 PM
 *
 * @author: a554998
 */
class BasicTestObject {
  String id;
  int intValue;
  String stringValue;

  public BasicTestObject() {
    new BasicTestObject("", 0, "");
  }

  public BasicTestObject(String id, int intValue, String stringValue) {
    this.id = id;
    this.intValue = intValue;
    this.stringValue = stringValue;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getIntValue() {
    return intValue;
  }

  public void setIntValue(int intValue) {
    this.intValue = intValue;
  }

  public String getStringValue() {
    return stringValue;
  }

  public void setStringValue(String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    return this.getId() + " / " + this.getIntValue() + " / " + this.getStringValue();
  }

  public DBObject asDBObject() {
    DBObject dbo = new BasicDBObject("_id", this.getId())
        .append("intValue", this.getIntValue())
        .append("stringValue", this.getStringValue());
    return dbo;
  }
}

