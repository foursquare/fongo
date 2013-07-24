package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.Fongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;
import java.util.UUID;

/**
 * User: william Date: 24/07/13
 */
public abstract class PipelineKeyword {

  protected static final DB fongo = new Fongo("aggregation_pipeline").getDB("pipeline");


  public abstract DBCollection apply(DBCollection coll, DBObject object);

  /**
   * Return the keywork in the pipeline (like $sort, $group...).
   *
   * @return
   */
  public abstract String getKeyword();

  /**
   * Drop collection and create new one with objects.
   *
   * @param coll
   * @param objects
   * @return the new collection.
   */
  protected DBCollection dropAndInsert(DBCollection coll, List<DBObject> objects) {
    coll.drop();
    coll = fongo.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(objects);
    return coll;
  }

  public boolean canApply(DBObject object) {
    return object.containsField(getKeyword());
  }
}