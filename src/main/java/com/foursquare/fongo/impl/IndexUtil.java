package com.foursquare.fongo.impl;

import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.util.ArrayList;
import java.util.Collection;
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

  public List<List<Object>> extractFields(DBObject dbObject, Collection<String> fields) {
    ExpressionParser expressionParser = new ExpressionParser();

    List<List<Object>> fieldValue = new ArrayList<List<Object>>();
    for (String field : fields) {
      List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
      fieldValue.add(embeddedValues);
    }
    return fieldValue;
  }

}
