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
public enum IndexUtil {
  INSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(IndexUtil.class);

  private IndexUtil() {
  }

  private List<List<List<Object>>> extractFields(Iterable<DBObject> objects, List<String> fields) {
    ExpressionParser expressionParser = new ExpressionParser();

    final List<List<List<Object>>> values = new ArrayList<List<List<Object>>>();
    for (DBObject dbObject : objects) {
      List<List<Object>> fieldValue = new ArrayList<List<Object>>();
      for (String field : fields) {
        List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
        fieldValue.add(embeddedValues);
      }
      if (!fieldValue.isEmpty()) {
        values.add(fieldValue);
      }
    }
    return values;
  }

  public List<List<Object>> extractFields(DBObject dbObject, List<String> fields) {
    ExpressionParser expressionParser = new ExpressionParser();

    List<List<Object>> fieldValue = new ArrayList<List<Object>>();
    for (String field : fields) {
      List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
      fieldValue.add(embeddedValues);
    }
    return fieldValue;
  }

  /**
   * Check if objects respect uniqueness of theses indexes.
   *
   * @param indexes
   * @param objects
   * @return null if all goes well, ask if the index problem.
   */
  public Index checkForUniqueness(Iterable<Index> indexes, Iterable<DBObject> objects) throws MongoException.DuplicateKey {
    for (Index index : indexes) {
      if (index.isUnique()) {
        List<List<List<Object>>> fieldsForIndex = extractFields(objects, index.getFields());
//        LOG.debug("for index {}, fields : {}", index.getName(), fieldsForIndex);
        Set<List<List<Object>>> set = new HashSet<List<List<Object>>>(fieldsForIndex);
        if (set.size() != fieldsForIndex.size()) {
          return index;
        }
      }
    }
    return null;
  }

}
