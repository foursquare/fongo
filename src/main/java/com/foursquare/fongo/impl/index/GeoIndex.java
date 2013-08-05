package com.foursquare.fongo.impl.index;

import com.foursquare.fongo.impl.Filter;
import com.foursquare.fongo.impl.GeoUtil;
import com.foursquare.fongo.impl.Util;
import com.github.davidmoten.geo.LatLong;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index for the MongoDB.
 */
public class GeoIndex extends IndexAbstract<GeoUtil.GeoDBObject> {
  static final Logger LOG = LoggerFactory.getLogger(GeoIndex.class);

  // EXPERIMENTAL SET TO FALSE : did not work well...
  static final boolean BRUTE_FORCE = true;

  GeoIndex(String name, DBObject keys, boolean unique, String geoIndex) {
    super(name, keys, unique, new TreeMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(new GeoUtil.GeoComparator(geoIndex)), geoIndex);
  }

  /**
   * Create the key for the hashmap.
   *
   * @param object
   * @return
   */
  @Override
  protected synchronized GeoUtil.GeoDBObject getKeyFor(DBObject object) {
    return new GeoUtil.GeoDBObject(super.getKeyFor(object), geoIndex);
  }

  @Override
  public GeoUtil.GeoDBObject clone(DBObject object) {
    return new GeoUtil.GeoDBObject(Util.clone(object), geoIndex);
  }

  public synchronized List<DBObject> geoNear(DBObject query, LatLong coordinate, int limit, boolean spherical) {
    lookupCount++;

    LOG.info("geoNear() query:{}, coordinate:{}, limit:{}, spherical:{}", query, coordinate, limit, spherical);
    Queue<String> hashs = new LinkedList<String>();
    hashs.add(GeoUtil.encodeGeoHash(coordinate));

    // Filter values
    Filter filterValue = expressionParser.buildFilter(query);

    List<DBObject> result = new ArrayList<DBObject>();

    if (BRUTE_FORCE) {
      // Bruteforce : try all.
      for (Map.Entry<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> entry : mapValues.entrySet()) {
        geoNearResults(entry.getValue(), filterValue, coordinate, result, spherical);
      }
    } else {

      Map<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> copyMap = new LinkedHashMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(mapValues);
      Set<String> visited = new HashSet<String>();

      // Do not break if limit is reached, because of neighbours.
      while (!hashs.isEmpty() && copyMap.size() != 0) {
        String hash = hashs.poll(); // get head
        if (!visited.contains(hash)) { // Do not try to visit if already done.
          visited.add(hash);

          for (Map.Entry<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> entry : copyMap.entrySet()) {
            GeoUtil.GeoDBObject geoObject = entry.getKey();
            // Hash is found
            if (geoObject.getGeoHash().startsWith(hash)) {
              // Can we apply other filter ?
              List<GeoUtil.GeoDBObject> values = entry.getValue();
              // Now transform to {dis:<distance>, obj:<result>}
              geoNearResults(values, filterValue, coordinate, result, spherical);
              copyMap.remove(entry.getKey());
              break;
            }
          }
          // Add more hash if limit is not reached.
          // If limit is out of scope, stop adding hash but continue to poll them from stack.
          // The order and sublist will be doing later.
          if (result.size() < limit * 10) {
            if (hash.length() >= 1) {
              try {
                hashs.addAll(GeoUtil.neightbours(hash));
              } catch (Exception e) {

              }
              hashs.add(hash.substring(0, hash.length() - 1)); // Look for larger size.
            }
          }
        }
      }
    }
    // Sort values by distance.
    Collections.sort(result, new Comparator<DBObject>() {
      @Override
      public int compare(DBObject o1, DBObject o2) {
        return ((Double) o1.get("dis")).compareTo((Double) o2.get("dis"));
      }
    });
    // Applying limit
    return result.subList(0, Math.min(result.size(), limit));
  }


  // Now transform to {dis:<distance>, obj:<result>}
  private void geoNearResults(List<GeoUtil.GeoDBObject> values, Filter filterValue, LatLong point, List<DBObject> result, boolean spherical) {
    for (DBObject v : values) {
      GeoUtil.GeoDBObject geoDBObject = (GeoUtil.GeoDBObject) v;
      // Test against the query filter.
      if (geoDBObject.getLatLong() != null && filterValue.apply(geoDBObject)) {
        double radians = GeoUtil.distanceInRadians(geoDBObject.getLatLong(), point, spherical);
        result.add(new BasicDBObject("dis", radians).append("obj", Util.clone(geoDBObject)));
      }
    }
  }

}
