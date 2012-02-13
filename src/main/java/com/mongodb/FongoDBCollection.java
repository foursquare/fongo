package com.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private final Map<Object, Integer> idIndex = new HashMap<Object, Integer>();
  
  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
  }
  
  @Override
  public synchronized WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    for (DBObject obj : arr) {
      fInsert(obj);
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }

  protected void fInsert(DBObject obj) {
    if (!obj.containsField(ID_KEY)) {
      obj.put(ID_KEY, new ObjectId());
    }
    Object id = obj.get(ID_KEY);
    Integer existingIndex = idIndex.get(id);
    if (existingIndex != null){
      objects.set(existingIndex, obj);
    } else {
      objects.add(obj);
      idIndex.put(id, objects.size() - 1);
    }
  }

  @Override
  public synchronized WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
      DBEncoder encoder) throws MongoException {
    if (o.containsField(ID_KEY)){
      throw new MongoException.DuplicateKey(0, "can't update " + ID_KEY);
    }
    final ExpressionParser expressionParser = new ExpressionParser();
    Filter filter = expressionParser.buildFilter(q);
    boolean wasFound = false;
    UpdateEngine updateEngine = new UpdateEngine(q, false);
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
      BasicDBObject newObject = createUpsertObject(q);
      fInsert(updateEngine.doUpdate(newObject, o));
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }

  protected  BasicDBObject createUpsertObject(DBObject q) {
    BasicDBObject newObject = new BasicDBObject();
    for (String key : q.keySet()){
      Object value = q.get(key);
      boolean okValue = true;
      if (value instanceof DBObject){
        for (String innerKey : ((DBObject) value).keySet()){
          if (innerKey.startsWith("$")){
            okValue = false;
          }
        }
      }
      if (okValue){
        newObject.put(key, value);
      }
    }
    return newObject;
  }

  @Override
  protected void doapply(DBObject o) {
  }

  @Override
  public synchronized WriteResult remove(DBObject o, WriteConcern concern, DBEncoder encoder) throws MongoException {
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
  synchronized Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
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
    List<DBObject> objectsToSearch = sortObjects(orderby, expressionParser);
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

  protected List<DBObject> sortObjects(DBObject orderby, final ExpressionParser expressionParser) {
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
    return objectsToSearch;
  }

  public synchronized int fCount(DBObject object) {
    return objects.size();
  }

  public synchronized DBObject fFindAndModify(DBObject query, DBObject update, DBObject sort, boolean remove,
      boolean returnNew, boolean upsert) {
    final ExpressionParser expressionParser = new ExpressionParser();
    Filter filter = expressionParser.buildFilter(query);
 
    List<DBObject> objectsToSearch = sortObjects(sort, expressionParser);
    DBObject beforeObject = null;
    DBObject afterObject = null;
    UpdateEngine updateEngine = new UpdateEngine(query, false);
    for (int i = 0; i < objectsToSearch.size() && beforeObject == null; i++) {
      DBObject dbo = objectsToSearch.get(i);
      if (filter.apply(dbo)) {
        beforeObject = dbo;
        if (!remove) {
          afterObject = new BasicDBObject();
          afterObject.putAll(beforeObject);
          fInsert(updateEngine.doUpdate(afterObject, update));
        } else {
          remove(dbo);
          return dbo;
        }
      }
    }
    if (beforeObject != null && !returnNew){
      return beforeObject;
    }
    if (beforeObject == null && upsert && !remove){
      afterObject = createUpsertObject(query);
      fInsert(updateEngine.doUpdate(afterObject, update));
    }
    return afterObject;
  }
  

}
