package com.mongodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.types.ObjectId;

import com.foursquare.fongo.ExpressionParser;
import com.foursquare.fongo.Filter;

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
      if (obj.get(ID_KEY) == null) {
        obj.put(ID_KEY, new ObjectId());
      }
      objects.add(obj);
    }
    return null;
  }

  @Override
  public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
      DBEncoder encoder) throws MongoException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void doapply(DBObject o) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public WriteResult remove(DBObject o, WriteConcern concern, DBEncoder encoder) throws MongoException {
    // TODO Auto-generated method stub
    return null;
  }



  @Override
  public void createIndex(DBObject keys, DBObject options, DBEncoder encoder) throws MongoException {
    // TODO Auto-generated method stub
    
  }

  @Override
  Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
      ReadPreference readPref, DBDecoder decoder) throws MongoException {
    ArrayList<DBObject> results = new ArrayList<DBObject>();
    Filter filter = new ExpressionParser().buildFilter(ref);
    int foundCount = 0;
    int upperLimit = Integer.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }
    for (int i = numToSkip; i < objects.size() && foundCount <= upperLimit; i++) {
      DBObject dbo = objects.get(i);
      if (filter.apply(dbo)) {
        foundCount++;
        results.add(dbo);
      }
    }
    return results.iterator();
  }

  public int fCount(DBObject object) {
    return objects.size();
  }
  

}
