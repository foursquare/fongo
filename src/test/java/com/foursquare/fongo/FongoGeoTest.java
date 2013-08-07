package com.foursquare.fongo;

import com.foursquare.fongo.impl.geo.GeoUtil;
import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class FongoGeoTest {

  public final FongoRule fongoRule = new FongoRule(!true);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(exception).around(fongoRule);

  @Test
  public void testCommandGeoNearWithFarDataSpherical() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(2.265D, 48.791D)).append("spherical", true));
    commandResult.throwOnError();

    DBObject results = (DBObject) commandResult.get("results");
    System.out.println(results);
    assertEquals(roundDis(Util.list(
        new BasicDBObject("dis", 0.0).append("obj", new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D))),
        new BasicDBObject("dis", 0.9152566098183758).append("obj", new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D))))), roundDis(results));
  }

  @Test
  public void testCommandGeoNearSpherical() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(2.2D, 48.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // Test geoNear with real distance
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(2.265D, 48.791D)).append("spherical", true));
    commandResult.throwOnError();

    DBObject results = (DBObject) commandResult.get("results");
    System.out.println(results);
    assertEquals(roundDis(Util.list(
        new BasicDBObject("dis", 0.0).append("obj", new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D))),
        new BasicDBObject("dis", 0.0014473989348370443).append("obj", new BasicDBObject("_id", 1).append("loc", Util.list(2.2D, 48.72D))))), roundDis(results));
  }

  @Test
  public void testCommandGeoNear2d() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // geoNear
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(40.0, 44.791D)));
    commandResult.throwOnError();
    System.out.println(commandResult);

    DBObject results = (DBObject) commandResult.get("results");
    assertEquals(roundDis(Util.list(
        new BasicDBObject("dis", 34.21306681664185).append("obj", new BasicDBObject("_id", 1).append("loc", Util.list(73.97D, 40.72D))),
        new BasicDBObject("dis", 37.94641254453445).append("obj", new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D))))), roundDis(results));
  }

  @Test
  public void testDistChangeAtLat() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(73D, 40D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2D, 48D)));
    collection.insert(new BasicDBObject("_id", 3).append("loc", Util.list(0D, 0D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // geoNear
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(40.0, 44D)));
    commandResult.throwOnError();
    System.out.println(commandResult);

    DBObject results = (DBObject) commandResult.get("results");
    assertEquals((Util.list(
        new BasicDBObject("dis", 33.24154027718932).append("obj", new BasicDBObject("_id", 1).append("loc", Util.list(73D, 40D))),
        new BasicDBObject("dis", 38.2099463490856).append("obj", new BasicDBObject("_id", 2).append("loc", Util.list(2D, 48D))),
        new BasicDBObject("dis", 59.464274989274024).append("obj", new BasicDBObject("_id", 3).append("loc", Util.list(0D, 0D))))), (results));
  }

  @Test
  public void testDistChangeAtLatSpherical() throws Exception {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(73D, 40D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2D, 48D)));
    collection.insert(new BasicDBObject("_id", 3).append("loc", Util.list(0D, 0D)));
    collection.insert(new BasicDBObject("_id", 4).append("loc", Util.list(-73D, -40D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // geoNear
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(40.0, 44D)).append("spherical", true));
    commandResult.throwOnError();
    System.out.println(commandResult);

    DBObject results = (DBObject) commandResult.get("results");
    System.out.println(results);
    assertEquals(Util.list(
        new BasicDBObject("dis", 0.43072310518886536).append("obj", new BasicDBObject("_id", 1).append("loc", Util.list(73D, 40D))),
        new BasicDBObject("dis", 0.46118276717032347).append("obj", new BasicDBObject("_id", 2).append("loc", Util.list(2D, 48D))),
        new BasicDBObject("dis", 0.9871788163259546).append("obj", new BasicDBObject("_id", 3).append("loc", Util.list(0D, 0D))),
        new BasicDBObject("dis", 2.2940518831146437).append("obj", new BasicDBObject("_id", 4).append("loc", Util.list(-73D, -40D)))), (results));
  }

  @Test
  public void testZip() throws Exception {
    DBCollection collection = fongoRule.insertFile(fongoRule.newCollection(), "/zips.json");
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // geoNear
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(-47.00, 44.919D)).append("limit", 10));
    commandResult.throwOnError();
    System.out.println(commandResult);
    DBObject results = (DBObject) commandResult.get("results");
    assertEquals(10, ((BasicDBList) results).size());
    System.out.println(results);

    assertEquals(fongoRule.parseDEObject("[ { \"dis\" : 20.007390023320283 , \"obj\" : { \"_id\" : \"04631\" , \"city\" : \"EASTPORT\" , \"loc\" : [ -67.00739 , 44.919966] , \"pop\" : 2514 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.04619295098797 , \"obj\" : { \"_id\" : \"04652\" , \"city\" : \"LUBEC\" , \"loc\" : [ -67.046016 , 44.834772] , \"pop\" : 2349 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.093003320979673 , \"obj\" : { \"_id\" : \"04667\" , \"city\" : \"PERRY\" , \"loc\" : [ -67.092882 , 44.988824] , \"pop\" : 781 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.143844748425007 , \"obj\" : { \"_id\" : \"04671\" , \"city\" : \"ROBBINSTON\" , \"loc\" : [ -67.143301 , 45.067007] , \"pop\" : 495 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.200257281293027 , \"obj\" : { \"_id\" : \"04666\" , \"city\" : \"PEMBROKE\" , \"loc\" : [ -67.200204 , 44.965396] , \"pop\" : 1151 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.22444395910023 , \"obj\" : { \"_id\" : \"04628\" , \"city\" : \"DENNYSVILLE\" , \"loc\" : [ -67.224431 , 44.896105] , \"pop\" : 684 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.25133524815737 , \"obj\" : { \"_id\" : \"04626\" , \"city\" : \"CUTLER\" , \"loc\" : [ -67.249869 , 44.67531] , \"pop\" : 779 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.265652799426036 , \"obj\" : { \"_id\" : \"04619\" , \"city\" : \"CALAIS\" , \"loc\" : [ -67.26408 , 45.171478] , \"pop\" : 3963 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.382831388484767 , \"obj\" : { \"_id\" : \"04630\" , \"city\" : \"EAST MACHIAS\" , \"loc\" : [ -67.382066 , 44.742362] , \"pop\" : 1574 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 20.383098806303718 , \"obj\" : { \"_id\" : \"04657\" , \"city\" : \"MEDDYBEMPS\" , \"loc\" : [ -67.382852 , 45.019306] , \"pop\" : 242 , \"state\" : \"ME\"}}]"), results);
  }

  @Test
  public void testZipSpherical() throws Exception {
    DBCollection collection = fongoRule.insertFile(fongoRule.newCollection(), "/zips.json");
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    // geoNear
    CommandResult commandResult = collection.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(-47.00, 44.919D)).append("limit", 10).append("spherical", true));
    commandResult.throwOnError();
    DBObject results = (DBObject) commandResult.get("results");
    assertEquals(10, ((BasicDBList) results).size());

    assertEquals(roundDis(fongoRule.parseDEObject("[ { \"dis\" : 0.24663504266786612 , \"obj\" : { \"_id\" : \"04631\" , \"city\" : \"EASTPORT\" , \"loc\" : [ -67.00739 , 44.919966] , \"pop\" : 2514 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.24729709096503652 , \"obj\" : { \"_id\" : \"04652\" , \"city\" : \"LUBEC\" , \"loc\" : [ -67.046016 , 44.834772] , \"pop\" : 2349 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.24753718895971535 , \"obj\" : { \"_id\" : \"04667\" , \"city\" : \"PERRY\" , \"loc\" : [ -67.092882 , 44.988824] , \"pop\" : 781 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.24799536619283583 , \"obj\" : { \"_id\" : \"04671\" , \"city\" : \"ROBBINSTON\" , \"loc\" : [ -67.143301 , 45.067007] , \"pop\" : 495 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.2489018370310107 , \"obj\" : { \"_id\" : \"04666\" , \"city\" : \"PEMBROKE\" , \"loc\" : [ -67.200204 , 44.965396] , \"pop\" : 1151 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.24927073571972508 , \"obj\" : { \"_id\" : \"04619\" , \"city\" : \"CALAIS\" , \"loc\" : [ -67.26408 , 45.171478] , \"pop\" : 3963 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.24934892984487747 , \"obj\" : { \"_id\" : \"04628\" , \"city\" : \"DENNYSVILLE\" , \"loc\" : [ -67.224431 , 44.896105] , \"pop\" : 684 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.2501775456106909 , \"obj\" : { \"_id\" : \"04626\" , \"city\" : \"CUTLER\" , \"loc\" : [ -67.249869 , 44.67531] , \"pop\" : 779 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.25102654021421605 , \"obj\" : { \"_id\" : \"04657\" , \"city\" : \"MEDDYBEMPS\" , \"loc\" : [ -67.382852 , 45.019306] , \"pop\" : 242 , \"state\" : \"ME\"}} ," +
        " { \"dis\" : 0.25105641737336887 , \"obj\" : { \"_id\" : \"04491\" , \"city\" : \"VANCEBORO\" , \"loc\" : [ -67.463419 , 45.558761] , \"pop\" : 217 , \"state\" : \"ME\"}}]")), roundDis(results));
  }

  @Test
  public void testFindByNearSphere() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$nearSphere", Util.list(2.297, 48.809)).append("$maxDistance", 3100D / GeoUtil.EARTH_RADIUS))).toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D))), objects);
  }

  @Test
  public void testFindByNearSphereNoMaxDistance() {
    DBCollection collection = fongoRule.newCollection();
    collection.ensureIndex(new BasicDBObject("loc", "2d"));
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(84.265D, 40.791D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(85.97D, 40.72D)));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$nearSphere",
        new BasicDBObject("$geometry", new BasicDBObject("type", "Point").append("coordinates", Util.list(-2.297, 48.809)))))).toArray();

    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1).append("loc", Util.list(84.265D, 40.791D)),
        new BasicDBObject("_id", 2).append("loc", Util.list(85.97D, 40.72D))), objects);
  }

  @Test
  @Ignore // TODO(twillouer) order by distance mvnwhen a $near or $nearSphere.
  public void testFindByNearSphereOrderedByDist() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(84.265D, 48.791D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$nearSphere",
        new BasicDBObject("$geometry", new BasicDBObject("type", "Point").append("coordinates", Util.list(2.297, 48.809)))))).toArray();

    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D)),
        new BasicDBObject("_id", 1).append("loc", Util.list(84.265D, 48.791D))), objects);
  }

  @Test
  public void testFindByNearSphereReturnOrderedByDist() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(2.265D, 48.791D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$nearSphere",
        new BasicDBObject("$geometry", new BasicDBObject("type", "Point").append("coordinates", Util.list(2.297, 48.809)))))).toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1).append("loc", Util.list(2.265D, 48.791D)),
        new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D))), objects);
  }

  @Test
  public void testFindByNear() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(-73.97D, 40.72D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$near", Util.list(2.297, 48.809)).append("$maxDistance", 50D))).toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 2).append("loc", Util.list(2.265D, 48.791D))), objects);
  }

  @Test
  public void testFindByNearNoMaxDistance() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("loc", Util.list(2.265D, 48.791D)));
    collection.insert(new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D)));
    collection.ensureIndex(new BasicDBObject("loc", "2d"));

    List<DBObject> objects = collection.find(new BasicDBObject("loc", new BasicDBObject("$near",
        new BasicDBObject("$geometry", new BasicDBObject("type", "Point").append("coordinates", Util.list(2.297, 48.809)))))).toArray();
    assertEquals(Arrays.asList(
        new BasicDBObject("_id", 1).append("loc", Util.list(2.265D, 48.791D)),
        new BasicDBObject("_id", 2).append("loc", Util.list(-73.97D, 40.72D))), objects);
  }

  private static DBObject roundDis(DBObject objectList) {
    for (DBObject o : (List<DBObject>) objectList) {
      o.put("dis", round((Double) o.get("dis")));
    }
    return objectList;
  }

  private static Double round(Double dis) {
    double mul = 1000000D;
    return Math.round(dis * mul) / mul;
  }

  //https://groups.google.com/forum/#!topic/mongodb-user/T4L3L7m0Cho
//  @Test
//  public void test
}