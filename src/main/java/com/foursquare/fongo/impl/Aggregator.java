package com.foursquare.fongo.impl;

import com.foursquare.fongo.impl.aggregation.Group;
import com.foursquare.fongo.impl.aggregation.Limit;
import com.foursquare.fongo.impl.aggregation.Match;
import com.foursquare.fongo.impl.aggregation.PipelineKeyword;
import com.foursquare.fongo.impl.aggregation.Project;
import com.foursquare.fongo.impl.aggregation.Skip;
import com.foursquare.fongo.impl.aggregation.Sort;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 22/07/13
 */
public class Aggregator {
  private static final Logger LOG = LoggerFactory.getLogger(Aggregator.class);

  private final FongoDB fongoDB;
  private final FongoDBCollection fongoDBCollection;
  private final List<DBObject> pipeline;
  private final List<PipelineKeyword> keywords;

  public Aggregator(FongoDB fongoDB, FongoDBCollection coll, List<DBObject> pipeline) {
    this.fongoDB = fongoDB;
    this.fongoDBCollection = coll;
    this.pipeline = pipeline;

    Match match = new Match(fongoDB);
    Project project = new Project(fongoDB);
    Group group = new Group(fongoDB);
    Sort sort = new Sort(fongoDB);
    Limit limit = new Limit(fongoDB);
    Skip skip = new Skip(fongoDB);
    this.keywords = Arrays.asList(match, project, group, sort, limit, skip);
  }

  /**
   * @return null if error.
   */
  public List<DBObject> computeResult() {
    DBCollection coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(this.fongoDBCollection.find().toArray());

    for (DBObject object : pipeline) {
      for(PipelineKeyword keyword : keywords) {
        if(keyword.canApply(object)) {
          coll = keyword.apply(coll, object);
          break;
        }
      }
    }

    List<DBObject> result = coll.find().toArray();
    coll.drop();

    LOG.debug("computeResult : {}", result);

    return result;
  }

  /**
   * Drop collection and create new one with objects.
   *
   * @param coll
   * @param objects
   * @return the new collection.
   */
  private DBCollection dropAndInsert(DBCollection coll, List<DBObject> objects) {
    coll.drop();
    coll = fongoDB.createCollection(UUID.randomUUID().toString(), null);
    coll.insert(objects);
    return coll;
  }
}
