package com.foursquare.fongo.impl;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class GeoUtil {

  public static final double EARTH_RADIUS = 6374892.5; // common way : 6378100D;

  //  Precision, Distance of Adjacent Cell in Meters
  //      1,     5003530
  //      2,     625441
  //      3,     123264
  //      4,     19545
  //      5,     3803
  //      6,     610
  //      7,     118
  //      8,     19
  //      9,     3.71
  //      10,    0.6
  public static int SIZE_GEOHASH = 5; // Size of the geohash. 12 = more accurate.

  private GeoUtil() {
  }

  public static class GeoDBObject extends BasicDBObject {
    private final String geoHash;
    private final LatLong latLong;

    public GeoDBObject(DBObject object, String indexKey) {
      BasicDBList list = (BasicDBList) object.get(indexKey);
      this.latLong = new LatLong((Double) list.get(1), (Double) list.get(0));
      this.geoHash = GeoUtil.encodeGeoHash(this.getLatLong());
      this.putAll(object);
    }

    public String getGeoHash() {
      return geoHash;
    }

    public LatLong getLatLong() {
      return latLong;
    }

    @Override
    public int hashCode() {
      return getGeoHash().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoDBObject)) return false;
      if (!super.equals(o)) return false;

      GeoDBObject that = (GeoDBObject) o;

      if (!getGeoHash().equals(that.getGeoHash())) return false;

      return true;
    }

    @Override
    public String toString() {
      return "GeoDBObject{" +
          "geoHash='" + getGeoHash() + '\'' +
          ", latLong=" + getLatLong() +
          '}';
    }
  }

  public static class GeoComparator implements Comparator<DBObject> {
    private final List<String> keyPath;
    private final ExpressionParser expressionParser = new ExpressionParser();

    public GeoComparator(String key) {
      this.keyPath = Util.split(key);
    }

    @Override
    public int compare(DBObject oo1, DBObject oo2) {
      GeoDBObject o1 = (GeoDBObject) oo1;
      GeoDBObject o2 = (GeoDBObject) oo2;
      int ret = o1.getGeoHash().compareTo(o2.getGeoHash());
      return ret;
    }
  }

  public static double distanceInRadians(LatLong p1, LatLong p2, boolean spherical) {
    double distance;
    if (spherical) {
      distance = distanceSpherical(p1, p2);
    } else {
      distance = distance2d(p1, p2);
    }
    return distance;
  }

  // Take me a day before I see this : https://github.com/mongodb/mongo/blob/ba239918c950c254056bf589a943a5e88fd4144c/src/mongo/db/geo/shapes.cpp
  public static double distance2d(LatLong p1, LatLong p2) {
    double a = p1.getLat() - p2.getLat();
    double b = p1.getLon() - p2.getLon();

    // Avoid numerical error if possible...
    if (a == 0) return Math.abs(b);
    if (b == 0) return Math.abs(a);

    return Math.sqrt((a * a) + (b * b));
  }

  public static double distanceSpherical(LatLong p1, LatLong p2) {
    double p1lat = Math.toRadians(p1.getLat()); // e
    double p1long = Math.toRadians(p1.getLon());    // f
    double p2lat = Math.toRadians(p2.getLat());         // g
    double p2long = Math.toRadians(p2.getLon());             // h

    double sinx1 = Math.sin(p1lat), cosx1 = Math.cos(p1lat);
    double siny1 = Math.sin(p1long), cosy1 = Math.cos(p1long);
    double sinx2 = Math.sin(p2lat), cosx2 = Math.cos(p2lat);
    double siny2 = Math.sin(p2long), cosy2 = Math.cos(p2long);

    double crossProduct = cosx1 * cosx2 * cosy1 * cosy2 + cosx1 * siny1 * cosx2 * siny2 + sinx1 * sinx2;
    if (crossProduct >= 1D || crossProduct <= -1D) {
      return crossProduct > 0 ? 0 : Math.PI;
    }

    return Math.acos(crossProduct);
  }

  public static List<LatLong> latLon(List<String> path, DBObject object) {
    ExpressionParser expressionParser = new ExpressionParser();
    List<LatLong> result = new ArrayList<LatLong>();

    List objects;
    if (path.isEmpty()) {
      objects = Collections.singletonList(object);
    } else {
      objects = expressionParser.getEmbeddedValues(path, object);
    }
    for (Object value : objects) {
      LatLong latLong = null;
      if (value instanceof BasicDBList) {
        BasicDBList list = (BasicDBList) value;
        if (list.size() == 2) {
          latLong = new LatLong(((Number) list.get(1)).doubleValue(), ((Number) list.get(0)).doubleValue());
        }
      } else if (value instanceof DBObject) {
        DBObject dbObject = (DBObject) value;
        if (dbObject.containsField("lon") && dbObject.containsField("lat")) {
          latLong = new LatLong(((Number) dbObject.get("lat")).doubleValue(), ((Number) dbObject.get("lon")).doubleValue());
        }
      }
      if (latLong != null) {
        result.add(latLong);
      }
    }
    return result;
  }

  public static String encodeGeoHash(LatLong latLong) {
    return encodeGeoHash(latLong, SIZE_GEOHASH);
  }

  public static String encodeGeoHash(LatLong latLong, int sizeHash) {
    return GeoHash.encodeHash(latLong, sizeHash); // The more, the merrier.
  }

  public static LatLong decodeGeoHash(String geoHash) {
    return GeoHash.decodeHash(geoHash);
  }

  public static List<String> neightbours(String geoHash) {
    List<String> results = new ArrayList<String>();
    try {
      results.add(GeoHash.left(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.right(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.top(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.bottom(geoHash));
    } catch (Exception e) {
    }
    return results;
  }

}
