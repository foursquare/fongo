package com.foursquare.fongo.impl;

import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to help with indexes.
 */
public final class IndexUtil {
  private static final Logger LOG = LoggerFactory.getLogger(IndexUtil.class);

  private IndexUtil() {
  }

  private static List<List<Object>> extractFields(Iterable<DBObject> objects, List<String> fields) {
    final List<List<Object>> values = new ArrayList<List<Object>>();
    for (DBObject dbObject : objects) {
      List<Object> fieldValue = new ArrayList<Object>();
      for (String field : fields) {
        Object value = extractFieldValue(field, dbObject);
        if (value != null) {
          fieldValue.add(value);
        }
      }
      if (!fieldValue.isEmpty()) {
        values.add(fieldValue);
      }
    }
    return values;
  }

  /**
   * @param fieldName
   * @param dbObject
   * @return null if not found.
   */
  private static Object extractFieldValue(String fieldName, DBObject dbObject) {
    if (dbObject.containsField(fieldName)) {
      // TODO : extract complicated structure like "product.quantity"
      return dbObject.get(fieldName);
    }
    return null;
  }

  /**
   * Check if objects respect uniqueness of theses indexes.
   *
   * @param indexes
   * @param objects
   * @return null if all goes well, ask if the index problem.
   */
  public static Index checkForUniqueness(Iterable<Index> indexes, Iterable<DBObject> objects) throws MongoException.DuplicateKey {
    for (Index index : indexes) {
      if (index.isUnique()) {
        List<List<Object>> fieldsForIndex = extractFields(objects, index.getFields());
        LOG.debug("for index {}, fields : {}", index.getName(), fieldsForIndex);
        Set<List<Object>> set = new HashSet<List<Object>>(fieldsForIndex);
        if (set.size() != fieldsForIndex.size()) {
          return index;
        }
      }
    }
    return null;
  }

}
