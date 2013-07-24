package com.foursquare.fongo.impl.aggregation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;

import java.util.List;
import java.util.UUID;

/**
 * User: william Date: 24/07/13
 */
public abstract class PipelineKeyword {

  protected final FongoDB fongoDB;

  protected PipelineKeyword(FongoDB fongoDB) {
    this.fongoDB = fongoDB;
  }


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
    coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(objects);
    return coll;
  }

  public boolean canApply(DBObject object) {
    return object.containsField(getKeyword());
  }
}