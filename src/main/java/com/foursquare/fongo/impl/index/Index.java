package com.foursquare.fongo.impl.index;

import com.foursquare.fongo.impl.ExpressionParser;
import com.foursquare.fongo.impl.Util;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index for the MongoDB.
 */
public class Index extends IndexAbstract<DBObject> {
  static final Logger LOG = LoggerFactory.getLogger(Index.class);

  Index(String name, DBObject keys, boolean unique) {
    super(name, keys, unique, createMap(keys, unique), null);
  }

  private static Map<DBObject, List<DBObject>> createMap(DBObject keys, boolean unique) {
    // Preserve order only for id.
    if (unique && keys.containsField(FongoDBCollection.ID_KEY) && keys.toMap().size() == 1) {
      return new LinkedHashMap<DBObject, List<DBObject>>();
    } else {
      return new TreeMap<DBObject, List<DBObject>>(new ExpressionParser().buildObjectComparator(isAsc(keys)));
    }
  }

  public DBObject embedded(DBObject object) {
    return object; // Important : do not clone, indexes share objects between them.
  }

}
