package com.foursquare.fongo.impl.aggregation;

import com.foursquare.fongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.ArrayList;
import java.util.List;
import org.bson.util.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Unwind extends PipelineKeyword {
  private static final Logger LOG = LoggerFactory.getLogger(Unwind.class);

  public static final Unwind INSTANCE = new Unwind();

  private Unwind() {
  }

  /**
   * {@see http://docs.mongodb.org/manual/reference/aggregation/unwind/#pipe._S_unwind}
   * <p/>
   * Note $unwind has the following behaviors:
   * <pre>
   * $unwind is most useful in combination with $group.
   * You may undo the effects of unwind operation with the $group pipeline operator.
   * If you specify a target field for $unwind that does not exist in an input document, the pipeline ignores the input document, and will generate no result documents.
   * If you specify a target field for $unwind that is not an array, db.collection.aggregate() generates an error.
   * If you specify a target field for $unwind that holds an empty array ([]) in an input document, the pipeline ignores the input document, and will generates no result documents.
   * </pre>
   *
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    String fieldName = object.get(getKeyword()).toString();
    if (!fieldName.startsWith("$")) {
      throw new MongoException(""); // TODO
    }
    fieldName = fieldName.substring(1);

    List<DBObject> result = new ArrayList<DBObject>();
    for (DBObject dbObject : coll.find().toArray()) {
      if (Util.containsField(dbObject, fieldName)) {
        Object oValue = Util.extractField(dbObject, fieldName);
        if (!(oValue instanceof BasicDBList)) {
          throw new MongoException(15978, "exception: $unwind:  value at end of field path must be an array");
        }
        BasicDBList list = (BasicDBList) oValue;
        for (Object sublist : list) {
          DBObject newValue = Util.clone(dbObject);
          Util.putValue(newValue, fieldName, sublist);
          newValue.removeField("_id"); // TODO _id must be the same (but Fongo doesn't handle)
          result.add(newValue);
        }
      }
    }
    return dropAndInsert(coll, result);
  }

  @Override
  public String getKeyword() {
    return "$unwind";
  }

  //exception: $unwind:  value at end of field path must be an array" , "code" : 15978
}
