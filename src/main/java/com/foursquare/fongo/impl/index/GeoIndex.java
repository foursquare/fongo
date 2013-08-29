package com.foursquare.fongo.impl.index;

import com.foursquare.fongo.impl.Filter;
import com.foursquare.fongo.impl.geo.GeoUtil;
import com.foursquare.fongo.impl.Util;
import com.foursquare.fongo.impl.geo.LatLong;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index for the MongoDB.
 * <p/>
 * TODO : more $geometry.
 */
public class GeoIndex extends IndexAbstract<GeoUtil.GeoDBObject> {
  static final Logger LOG = LoggerFactory.getLogger(GeoIndex.class);

  // EXPERIMENTAL SET TO FALSE : did not work well...
  static final boolean BRUTE_FORCE = true;

  GeoIndex(String name, DBObject keys, boolean unique, String geoIndex) {
    super(name, keys, unique, new LinkedHashMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(), geoIndex);
    //TreeMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(new GeoUtil.GeoComparator(geoIndex)), geoIndex);
  }

  /**
   * Create the key for the hashmap.
   *
   * @param object
   * @return
   */
  @Override
  protected GeoUtil.GeoDBObject getKeyFor(DBObject object) {
    return new GeoUtil.GeoDBObject(super.getKeyFor(object), geoIndex);
  }

  @Override
  public GeoUtil.GeoDBObject embedded(DBObject object) {
    return new GeoUtil.GeoDBObject(object, geoIndex); // Important : do not clone, indexes share objects between them.
  }

  public List<DBObject> geoNear(DBObject query, List<LatLong> coordinates, int limit, boolean spherical) {
    lookupCount++;

    LOG.info("geoNear() query:{}, coordinate:{}, limit:{}, spherical:{} (mapValues size:{})", query, coordinates, limit, spherical, mapValues.size());
    // Filter values
    Filter filterValue = expressionParser.buildFilter(query);

    // Preserve order and remove duplicates.
    LinkedHashSet<DBObject> resultSet = new LinkedHashSet<DBObject>();

    if (BRUTE_FORCE) {
      // Bruteforce : try all.
      geoNearCoverAll(mapValues, filterValue, coordinates, spherical, resultSet);
    } else {
      Queue<String> hashs = new LinkedList<String>();
      for (LatLong coordinate : coordinates) {
        hashs.add(GeoUtil.encodeGeoHash(coordinate));
        hashs.addAll(GeoUtil.neightbours(GeoUtil.encodeGeoHash(coordinate)));
      }

      Map<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> copyMap = new LinkedHashMap<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>>(mapValues);
      Set<String> visited = new HashSet<String>();

      int sizeHash = GeoUtil.SIZE_GEOHASH - 3;
      // Do not break if limit is reached, because of neighbours.
      while (!hashs.isEmpty() && copyMap.size() != 0) {
        String hash = hashs.poll(); // get head
        if (!visited.contains(hash)) { // Do not try to visit if already done.
          visited.add(hash);

          for (Map.Entry<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> entry : copyMap.entrySet()) {
            GeoUtil.GeoDBObject geoObject = entry.getKey();
            // Hash is found
            LOG.info("compare {} and {}", geoObject.getGeoHash(), hash);
            if (geoObject.getGeoHash().startsWith(hash)) {
              LOG.info("gotcha : {}", geoObject);
              // Can we apply other filter ?
              List<GeoUtil.GeoDBObject> values = entry.getValue();
              // Now transform to {dis:<distance>, obj:<result>}
              for (LatLong coordinate : coordinates) {
                geoNearResults(values, filterValue, coordinate, resultSet, spherical);
              }
//              copyMap.remove(entry.getKey());
//              break;
            }
          }
          // Add more hash if limit is not reached.
          // If limit is out of scope, stop adding hash but continue to poll them from stack.
          // The order and sublist will be doing later.
          if (resultSet.size() < limit && sizeHash > 1) {
            if (hash.length() >= 1) {
              hashs.addAll(GeoUtil.neightbours(hash));
              LatLong reversed = GeoUtil.decodeGeoHash(hash);
              LOG.info("reversed:{}, hash:{}", reversed, hash);
              hashs.add(GeoUtil.encodeGeoHash(reversed, sizeHash)); // Look for larger size.
            }
          }
        }
      }
    }

    return sortAndLimit(resultSet, limit);
  }

  /**
   * Try all the map, without trying to filter by geohash.
   */
  private void geoNearCoverAll(Map<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> values, Filter filterValue, List<LatLong> coordinates, boolean spherical, LinkedHashSet<DBObject> resultSet) {
    for (Map.Entry<GeoUtil.GeoDBObject, List<GeoUtil.GeoDBObject>> entry : values.entrySet()) {
      for (LatLong coordinate : coordinates) {
        geoNearResults(entry.getValue(), filterValue, coordinate, resultSet, spherical);
      }
    }
  }

  /**
   * Sort the results and limit them.
   */
  private List<DBObject> sortAndLimit(Collection<DBObject> resultSet, int limit) {
    List<DBObject> result = new ArrayList<DBObject>(resultSet);
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
  private void geoNearResults(List<GeoUtil.GeoDBObject> values, Filter filterValue, LatLong point, Collection<DBObject> result, boolean spherical) {
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
