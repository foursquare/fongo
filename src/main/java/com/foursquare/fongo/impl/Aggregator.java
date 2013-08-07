package com.foursquare.fongo.impl;

import com.foursquare.fongo.impl.aggregation.Group;
import com.foursquare.fongo.impl.aggregation.Limit;
import com.foursquare.fongo.impl.aggregation.Match;
import com.foursquare.fongo.impl.aggregation.PipelineKeyword;
import com.foursquare.fongo.impl.aggregation.Project;
import com.foursquare.fongo.impl.aggregation.Skip;
import com.foursquare.fongo.impl.aggregation.Sort;
import com.foursquare.fongo.impl.aggregation.Unwind;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle the aggregation of a collection.
 */
public class Aggregator {
  private static final Logger LOG = LoggerFactory.getLogger(Aggregator.class);

  private final FongoDB fongoDB;
  private final FongoDBCollection fongoDBCollection;
  private final List<DBObject> pipeline;
  private static final List<PipelineKeyword> keywords = Arrays.asList(Match.INSTANCE, Project.INSTANCE, Group.INSTANCE, Sort.INSTANCE, Limit.INSTANCE, Skip.INSTANCE, Unwind.INSTANCE);

  public Aggregator(FongoDB fongoDB, FongoDBCollection coll, List<DBObject> pipeline) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.pipeline = pipeline;
  }

  /**
   * @return null if error.
   */
  public List<DBObject> computeResult() {
    DBCollection coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    try {
      coll.insert(this.fongoDBCollection.find().toArray());

      for (DBObject object : pipeline) {
        boolean found = false;
        for (PipelineKeyword keyword : keywords) {
          if (keyword.canApply(object)) {
            coll = keyword.apply(coll, object);
            found = true;
            break;
          }
        }
        if (!found) {
          fongoDB.errorResult(16436, "exception: Unrecognized pipeline stage name: '" + object.keySet() + "'").throwOnError();
        }
        // Not found : com.mongodb.CommandFailureException: { "serverUsed" : "localhost/127.0.0.1:27017" , "errmsg" : "exception: Unrecognized pipeline stage name: '_id'" , "code" : 16436 , "ok" : 0.0}
      }

      List<DBObject> result = coll.find().toArray();
      LOG.debug("computeResult() : {}", result);
      return result;
    } finally {
      coll.drop();
    }
  }
}
