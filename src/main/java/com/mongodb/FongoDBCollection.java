package com.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.foursquare.fongo.UpdateEngine;
import com.foursquare.fongo.Util;
import com.mongodb.FongoDBCollection.ObjectComparator;

public class FongoDBCollection extends DBCollection {

  final static String ID_KEY = "_id";
  private final FongoDB fongoDb;
  // LinkedHashMap maintains insertion order
  // TODO(jon) separate _id index from storage
  private final Map<Object, DBObject> objects = new LinkedHashMap<Object, DBObject>();
  private final ExpressionParser expressionParser;
  private final UpdateEngine updateEngine;
  private final boolean isDebug;
  private final boolean nonIdCollection;
  private final ObjectComparator objectComparator;
  
  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
    this.isDebug = db.isDebug();
    this.nonIdCollection = name.startsWith("system");
    this.expressionParser = new ExpressionParser(isDebug);
    this.updateEngine = new UpdateEngine(isDebug);
    this.objectComparator = new ObjectComparator();
  }
  
  class ObjectComparator implements Comparator {
    @Override
    public int compare(Object o1, Object o2) {
      return expressionParser.compareObjects(o1, o2);
    }
  }
  
  @Override
  public synchronized WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    for (DBObject obj : arr) {
      if (isDebug){
        debug("insert: " + obj);
      }
      filterLists(obj);
      Object id = putIdIfNotPresent(obj);

      if (objects.containsKey(id)) {
        if (enforceDuplicates(concern)) {
          throw new MongoException.DuplicateKey(0, "Attempting to insert duplicate _id: " + id);          
        } else {
          // TODO(jon) log          
        }
      } else {
        putSizeCheck(id, obj);        
      }
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }
  
  boolean enforceDuplicates(WriteConcern concern) {
    return !(WriteConcern.NONE.equals(concern) || WriteConcern.NORMAL.equals(concern));
  }

  public Object putIdIfNotPresent(DBObject obj) {
    if (!obj.containsField(ID_KEY)) {
      ObjectId id = new ObjectId();
      if (!nonIdCollection){
        obj.put(ID_KEY, id);
      }
      return id;
    } else {
      return obj.get(ID_KEY);
    }
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
      Object replacementValue = replaceListAndMap(value);
      dbo.put(key, replacementValue);
    }
    return dbo;
  }

  public Object replaceListAndMap(Object value) {
    Object replacementValue = value;
    if (value instanceof DBObject) {
      replacementValue = filterLists((DBObject) value);
    } else if (value instanceof List){
      BasicDBList list = new BasicDBList();
      for (Object listItem : (List) value){
        list.add(replaceListAndMap(listItem));
      }
      replacementValue = list;
    } else if (value instanceof Map) {
      BasicDBObject newDbo = new BasicDBObject();
      for (Map.Entry<String, Object>entry : (Set<Map.Entry<String, Object>>)((Map)value).entrySet()) {
        newDbo.put(entry.getKey(), replaceListAndMap(entry.getValue()));
      }
      replacementValue = newDbo;
    }
    return replacementValue;
  }


  protected void fInsert(DBObject obj) {
    putIdIfNotPresent(obj);
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
      throw new MongoException.DuplicateKey(0, "can not change _id of a document " + ID_KEY);
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
      List idsIn = idsIn(q);
      if (idOnlyUpdate && idsIn.size() > 0) {
        for (Object id : idsIn){
          DBObject existingObject = objects.get(id);
          if (existingObject != null){
            wasFound = true;
            updateEngine.doUpdate(existingObject, o, q);
            if (!multi){
              break;
            }
          }
        }
      } else {
        Filter filter = expressionParser.buildFilter(q);
        for (DBObject obj : objects.values()) {
          if (filter.apply(obj)){
            wasFound = true;
            updateEngine.doUpdate(obj, o, q);
            if (!multi){
              break;
            }
          }
        }
      }
      if (!wasFound && upsert){
        BasicDBObject newObject = createUpsertObject(q);
        fInsert(updateEngine.doUpdate(newObject, o, q));
      }
    }
    return new WriteResult(fongoDb.okResult(), concern);
  }
  

  
  
  public List idsIn(DBObject query) {
    Object idValue = query.get(ID_KEY);
    if (idValue == null || query.keySet().size() > 1) {
      return Collections.emptyList();
    } else if (idValue instanceof DBObject ){
      DBObject idDbObject = (DBObject)idValue;
      List inList = (List)idDbObject.get(ExpressionParser.IN);
      
      // I think sorting the inputed keys is a rough
      // approximation of how mongo creates the bounds for walking
      // the index.  It has the desired affect of returning results
      // in _id index order, but feels pretty hacky.
      if (inList != null){
        Object[] inListArray = inList.toArray(new Object[0]);
        // ids could be DBObjects, so we need a comparator that can handle that
        Arrays.sort(inListArray, objectComparator);
        return Arrays.asList(inListArray);
      }
      if (!isNotUpdateCommand(idValue)){
        return Collections.emptyList();
      }
    } 
    return Collections.singletonList(idValue);
  }

  protected  BasicDBObject createUpsertObject(DBObject q) {
    BasicDBObject newObject = new BasicDBObject();
    List idsIn = idsIn(q);
    
    if (!idsIn.isEmpty()){
      newObject.put(ID_KEY, idsIn.get(0));
    } else {
      BasicDBObject filteredQuery = new BasicDBObject();
      for (String key : q.keySet()){
        Object value = q.get(key);
        if (isNotUpdateCommand(value)){
          filteredQuery.put(key, value);
        }
      }
      updateEngine.mergeEmbeddedValueFromQuery(newObject, filteredQuery);
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
    List idList = idsIn(o);
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
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    BasicDBObject rec = new BasicDBObject();
    rec.append("v", 1);
    rec.append("key", keys);
    rec.append("ns", this.getDB().getName() + "." + this.getName());
    StringBuilder sb = new StringBuilder();
    boolean firstLoop = true;
    for (String keyName : keys.keySet()) {
      if (!firstLoop){
        sb.append("_");
      }
      sb.append(keyName).append("_").append(keys.get(keyName));
      firstLoop = false;
    }
    rec.append("name", sb.toString());
    rec.putAll(options);
    indexColl.insert(rec);
  }

  
  @Override
  synchronized Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
      ReadPreference readPref, DBDecoder decoder) throws MongoException {
    if (isDebug){
      debug("find(" + ref + ").limit("+limit+").skip("+numToSkip+")");
      debug("the db looks like " + objects);
    }
    List idList = idsIn(ref);
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
      Collection<DBObject> objectsToSearch = sortObjects(orderby);
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
    if (isDebug){
      debug("found results " + results);
    }
    if (results.size() == 0){
      return null;
    } else {
      return results.iterator();      
    }
  }

  public Collection<DBObject> sortObjects(final DBObject orderby) {
    Collection<DBObject> objectsToSearch = objects.values();
    if (orderby != null) {
      final Set<String> orderbyKeySet = orderby.keySet();
      if (!orderbyKeySet.isEmpty()){
        DBObject[] objectsToSort = objects.values().toArray(new DBObject [0]);

        Arrays.sort(objectsToSort, new Comparator<DBObject>(){
          @Override
          public int compare(DBObject o1, DBObject o2) {
            for (String sortKey : orderbyKeySet) {
              final List<String> path = Util.split(sortKey);
              int sortDirection = (Integer)orderby.get(sortKey);

              List<Object> o1list = expressionParser.getEmbeddedValues(path, o1);
              List<Object> o2list = expressionParser.getEmbeddedValues(path, o2);
              
              int compareValue = expressionParser.compareLists(o1list, o2list) * sortDirection;
              if (compareValue != 0){
                return compareValue;
              }
            }
            return 0;
          }});
        objectsToSearch = Arrays.asList(objectsToSort);
      }
    }
    if (isDebug) {
      debug("sorted objectsToSearch " + objectsToSearch);
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

    Collection<DBObject> objectsToSearch = sortObjects(sort);
    DBObject beforeObject = null;
    DBObject afterObject = null;
    for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext();) {
      DBObject dbo = iter.next();
      if (filter.apply(dbo)) {
        beforeObject = dbo;
        if (!remove) {
          afterObject = new BasicDBObject();
          afterObject.putAll(beforeObject);
          fInsert(updateEngine.doUpdate(afterObject, update, query));
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
      beforeObject = new BasicDBObject();
      afterObject = createUpsertObject(query);
      fInsert(updateEngine.doUpdate(afterObject, update, query));
    }
    if (returnNew){
      return afterObject;
    } else {
      return beforeObject;
    }
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
