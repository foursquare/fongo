package com.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import com.foursquare.fongo.ExpressionParser;
import com.foursquare.fongo.Filter;
import com.foursquare.fongo.FongoException;
import com.foursquare.fongo.Option;
import com.foursquare.fongo.UpdateEngine;

public class FongoDBCollection extends DBCollection {
  
  private final class IdComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      return expressionParser.compareObjects(o1, o2);
    }
  }

  final static String ID_KEY = "_id";
  private final FongoDB fongoDb;
  private final Map<Object, DBObject> objects = new LinkedHashMap<Object, DBObject>();
  private final ExpressionParser expressionParser = new ExpressionParser();
  private final boolean isDebug;
  
  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
    this.isDebug = db.isDebug();
  }
  
  @Override
  public synchronized WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    for (DBObject obj : arr) {
      if (isDebug){
        debug("insert: " + obj);
      }
      filterLists(obj);
      if (!obj.containsField(ID_KEY)) {
        obj.put(ID_KEY, new ObjectId());
      }
      Object id = obj.get(ID_KEY);
      if (objects.containsKey(id)){
        throw new MongoException.DuplicateKey(0, "Attempting to insert duplicate _id: " + id);
      }
      putSizeCheck(id, obj);
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }

  public void putSizeCheck(Object id, DBObject obj) {
    if (objects.size() > 100000) {
      throw new FongoException("Whoa, hold up there.  Fongo's designed for lightweight testing.  100,000 items per collection max");
    }
    objects.put(id, obj);
  }
  
  public DBObject filterLists(DBObject dbo){
    if (dbo == null) {
      return null;
    }
    for (String key : dbo.keySet()) {
      Object value = dbo.get(key);
      Object replacementValue = replaceList(value);
      dbo.put(key, replacementValue);
    }
    return dbo;
  }

  public Object replaceList(Object value) {
    Object replacementValue = value;
    if (value instanceof DBObject) {
      replacementValue = filterLists((DBObject) value);
    } else if (value instanceof List && !(value instanceof BasicDBList)){
      BasicDBList list = new BasicDBList();
      for (Object listItem : (List) value){
        list.add(replaceList(listItem));
      }
      replacementValue = list;
    }
    return replacementValue;
  }


  protected void fInsert(DBObject obj) {
    if (!obj.containsField(ID_KEY)) {
      obj.put(ID_KEY, new ObjectId());
    }
    Object id = obj.get(ID_KEY);
    putSizeCheck(id, obj);
  }

  void debug(String message) {
    if (isDebug){
      System.out.println("Fongo." + getName() + " " + message);
    }
  }
  @Override
  public synchronized WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
      DBEncoder encoder) throws MongoException {
    if (isDebug){
      debug("update(" + q + ", " + o + ", " + upsert + ", " + multi +")");
    }
    boolean idOnlyUpdate = q.containsField(ID_KEY) && q.keySet().size() == 1;
    if (o.containsField(ID_KEY) && !idOnlyUpdate){
      throw new MongoException.DuplicateKey(0, "can't update " + ID_KEY);
    }
    filterLists(o);
    if (idOnlyUpdate && isNotUpdateCommand(o)) {
      if (!o.containsField(ID_KEY)) {
        o.put(ID_KEY, q.get(ID_KEY));
      }
      fInsert(o);
    } else {
      filterLists(q);
      boolean wasFound = false;
      UpdateEngine updateEngine = new UpdateEngine(q, isDebug);
      if (idOnlyUpdate) {
        List<Object> ids = idsIn(q);
        if (!ids.isEmpty()){
          Object id = ids.get(0);
          DBObject existingObject = objects.get(id);
          if (existingObject != null){
            wasFound = true;
            updateEngine.doUpdate(existingObject, o);
          }
        }
      } else {
        Filter filter = expressionParser.buildFilter(q);
        for (DBObject obj : objects.values()) {
          if (filter.apply(obj)){
            wasFound = true;
            updateEngine.doUpdate(obj, o);
            if (!multi){
              break;
            }
          }
        }
      }
      if (!wasFound && upsert){
        BasicDBObject newObject = createUpsertObject(q);
        fInsert(updateEngine.doUpdate(newObject, o));
      }
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }
  
  public List<Object> idsIn(DBObject query) {
    Object idValue = query.get(ID_KEY);
    if (idValue == null || query.keySet().size() > 1) {
      return Collections.emptyList();
    } else if (idValue instanceof DBObject ){
      DBObject idDbObject = (DBObject)idValue;
      List inList = (List)idDbObject.get(ExpressionParser.IN);
      
      if (inList != null){
        return inList;
      }
      if (!isNotUpdateCommand(idValue)){
        return Collections.emptyList();
      }
    } 
    return Collections.singletonList(idValue);
  }

  protected  BasicDBObject createUpsertObject(DBObject q) {
    BasicDBObject newObject = new BasicDBObject();
    List<Object> idsIn = idsIn(q);
    
    if (!idsIn.isEmpty()){
      newObject.put(ID_KEY, idsIn.get(0));
    } else {
      for (String key : q.keySet()){
        Object value = q.get(key);
        boolean okValue = isNotUpdateCommand(value);
        if (okValue){
          newObject.put(key, value);
        }
      }
    }
    return newObject;
  }

  public boolean isNotUpdateCommand(Object value) {
    boolean okValue = true;
    if (value instanceof DBObject){
      for (String innerKey : ((DBObject) value).keySet()){
        if (innerKey.startsWith("$")){
          okValue = false;
        }
      }
    }
    return okValue;
  }

  @Override
  protected void doapply(DBObject o) {
  }

  @Override
  public synchronized WriteResult remove(DBObject o, WriteConcern concern, DBEncoder encoder) throws MongoException {
    if (isDebug){
      debug("remove: " + o);
    }
    List<Object> idList = idsIn(o);
    if (!idList.isEmpty()) {
      for (Object id : idList){
        objects.remove(id);        
      }
    } else {
      Filter filter = expressionParser.buildFilter(o);
      for (Iterator<DBObject> iter = objects.values().iterator(); iter.hasNext(); ) {
        DBObject dbo = iter.next();
        if (filter.apply(dbo)){
          iter.remove();
        }
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
    if (isDebug){
      debug("find(" + ref + ")");
    }
    List<Object> idList = idsIn(ref);
    ArrayList<DBObject> results = new ArrayList<DBObject>();
    if (!idList.isEmpty()) {
      if (isDebug){
        debug("find using id index only " + idList);
      }
      for (Object id : idList){
        DBObject result = objects.get(id);
        if (result != null){
          results.add(result);          
        }
      }
    } else {
      DBObject orderby = null;
      if (ref.containsField("query") && ref.containsField("orderby")) {
        orderby = (DBObject)ref.get("orderby");
        ref = (DBObject)ref.get("query");
      }
      
      Filter filter = expressionParser.buildFilter(ref);
      int foundCount = 0;
      int upperLimit = Integer.MAX_VALUE;
      if (limit > 0) {
        upperLimit = limit;
      }
      int seen = 0;
      Collection<DBObject> objectsToSearch = sortObjects(orderby, expressionParser);
      for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext() && foundCount <= upperLimit; seen++) {
        DBObject dbo = iter.next();
        if (seen >= numToSkip){
          if (filter.apply(dbo)) {
            foundCount++;
            results.add(dbo);
          }
        }
      }
    }
    if (results.size() == 0){
      return null;
    } else {
      return results.iterator();      
    }
  }

  protected Collection<DBObject> sortObjects(DBObject orderby, final ExpressionParser expressionParser) {
    Collection<DBObject> objectsToSearch = objects.values();
    if (orderby != null) {
      Set<String> orderbyKeys = orderby.keySet();
      if (!orderbyKeys.isEmpty()){
        final String sortKey = orderbyKeys.iterator().next();
        final int sortDirection = (Integer)orderby.get(sortKey);
        ArrayList<DBObject> objectList = new ArrayList<DBObject>(objects.values());
        Collections.sort(objectList, new Comparator<DBObject>(){
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
        return objectList;
      }
    }
    return objectsToSearch;
  }
  
  @Override
  public synchronized long getCount(DBObject query, DBObject fields, long limit, long skip ) {
    Filter filter = expressionParser.buildFilter(query);
    long count = 0;
    long upperLimit = Long.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }
    int seen = 0;
    for (Iterator<DBObject> iter = objects.values().iterator(); iter.hasNext() && count <= upperLimit;) {
      DBObject value = iter.next();
      if (seen++ >= skip) {
        if (filter.apply(value)){
          count++;
        }
      }
    }
    return count;
  }

  public synchronized DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert) {
    filterLists(query);
    filterLists(update);
    Filter filter = expressionParser.buildFilter(query);

    Collection<DBObject> objectsToSearch = sortObjects(sort, expressionParser);
    DBObject beforeObject = null;
    DBObject afterObject = null;
    UpdateEngine updateEngine = new UpdateEngine(query, isDebug);
    for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext();) {
      DBObject dbo = iter.next();
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
  
  @Override
  public synchronized List<DBObject> distinct(String key, DBObject query) {
    List<DBObject> results = new ArrayList<DBObject>();
    Filter filter = expressionParser.buildFilter(query);
    Set<Object> seen = new HashSet<Object>();
    for (Iterator<DBObject> iter = objects.values().iterator(); iter.hasNext();) {
      DBObject value = iter.next();
      if (filter.apply(value) && seen.add(value.get(key))){
        results.add(value);
      }
    }
    return results;
  }
  
  @Override
  public void dropIndexes(String name) throws MongoException {
    // do nothing
  }
}
