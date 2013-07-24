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
public class Match extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Match.class);

  public static final Match INSTANCE = new Match();

  private Match() {
  }

  /**
   * {@see http://docs.mongodb.org/manual/reference/aggregation/match/#pipe._S_match}
   *
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    LOG.debug("computeResult() match : {}", object);

    List<DBObject> objects = coll.find((DBObject) object.get(getKeyword())).toArray();
    coll = dropAndInsert(coll, objects);
    LOG.debug("computeResult() match : {}, result : {}", object, objects);
    return coll;
  }

  @Override
  public String getKeyword() {
    return "$match";
  }

}
