package com.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;

import com.foursquare.fongo.ExpressionParser;
import com.foursquare.fongo.Filter;
import com.foursquare.fongo.Option;
import com.foursquare.fongo.UpdateEngine;

public class FongoDBCollection extends DBCollection {
  final static String ID_KEY = "_id";
  private final FongoDB fongoDb;
  private final List<DBObject> objects = new ArrayList<DBObject>();
  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
  }
  
  @Override
  public WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    for (DBObject obj : arr) {
      if (!obj.containsField(ID_KEY)) {
        obj.put(ID_KEY, new ObjectId());
      }
      objects.add(obj);
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }

  @Override
  public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
      DBEncoder encoder) throws MongoException {
    if (o.containsField(ID_KEY)){
      throw new MongoException("can't update " + ID_KEY);
    }
    final ExpressionParser expressionParser = new ExpressionParser();
    Filter filter = expressionParser.buildFilter(q);
    boolean wasFound = false;
    UpdateEngine updateEngine = new UpdateEngine();
    for (DBObject obj : objects) {
      if (filter.apply(obj)){
        wasFound = true;
        updateEngine.doUpdate(obj, o);
        if (!multi){
          break;
        }
      }
    }
    if (!wasFound && upsert){
      insert(updateEngine.doUpdate(new BasicDBObject(), o));
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }

  @Override
  protected void doapply(DBObject o) {
  }

  @Override
  public WriteResult remove(DBObject o, WriteConcern concern, DBEncoder encoder) throws MongoException {
    final ExpressionParser expressionParser = new ExpressionParser();
    Filter filter = expressionParser.buildFilter(o);
    for (Iterator<DBObject> iter = objects.iterator(); iter.hasNext(); ) {
      DBObject dbo = iter.next();
      if (filter.apply(dbo)){
        iter.remove();
      }
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }



  @Override
  public void createIndex(DBObject keys, DBObject options, DBEncoder encoder) throws MongoException {
    // TODO Auto-generated method stub
    
  }

  @Override
  Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
      ReadPreference readPref, DBDecoder decoder) throws MongoException {
    ArrayList<DBObject> results = new ArrayList<DBObject>();
    DBObject orderby = null;
    if (ref.containsField("query") && ref.containsField("orderby")) {
      orderby = (DBObject)ref.get("orderby");
      ref = (DBObject)ref.get("query");
    }
    final ExpressionParser expressionParser = new ExpressionParser();
    Filter filter = expressionParser.buildFilter(ref);
    int foundCount = 0;
    int upperLimit = Integer.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }
    List<DBObject> objectsToSearch = objects;
    if (orderby != null) {
      Set<String> orderbyKeys = orderby.keySet();
      if (!orderbyKeys.isEmpty()){
        final String sortKey = orderbyKeys.iterator().next();
        final int sortDirection = (Integer)orderby.get(sortKey);
        objectsToSearch = new ArrayList<DBObject>(objects);
        Collections.sort(objectsToSearch, new Comparator<DBObject>(){
          @Override
          public int compare(DBObject o1, DBObject o2) {
            Option<Object> o1option = expressionParser.getEmbeddedValue(sortKey, o1);
            Option<Object> o2option = expressionParser.getEmbeddedValue(sortKey, o2);
            if (o1option.isEmpty()) {
              return -1 * sortDirection;
            } else if (o2option.isEmpty()) {
              return sortDirection;
            } else {
              Comparable o1Value = expressionParser.typecast(sortKey, o1option.get(), Comparable.class);
              Comparable o2Value = expressionParser.typecast(sortKey, o2option.get(), Comparable.class);
              
              return o1Value.compareTo(o2Value) * sortDirection;
            }
          }});
      }
    }
    for (int i = numToSkip; i < objectsToSearch.size() && foundCount <= upperLimit; i++) {
      DBObject dbo = objectsToSearch.get(i);
      if (filter.apply(dbo)) {
        foundCount++;
        results.add(dbo);
      }
    }
    if (results.size() == 0){
      return null;
    } else {
      return results.iterator();      
    }
  }

  public int fCount(DBObject object) {
    return objects.size();
  }
  

}
