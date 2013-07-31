package com.foursquare.fongo.impl.aggregation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;
import org.bson.util.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Sort extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Sort.class);

  public static final Sort INSTANCE = new Sort();

  private Sort() {
  }

  /**
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    List<DBObject> objects = coll.find().sort((DBObject) object.get(getKeyword())).toArray();
    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$sort";
  }

}
