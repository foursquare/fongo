package com.foursquare.fongo.impl.aggregation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 24/07/13
 */
public class Sort extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Sort.class);

  public Sort(FongoDB fongoDB) {
    super(fongoDB);
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
