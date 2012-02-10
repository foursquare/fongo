package com.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBDecoder;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class FongoDBCollection extends DBCollection {

  private final FongoDB fongoDb;
  private final List<DBObject> objects = new ArrayList<DBObject>();
  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
  }
  
  @Override
  public WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    objects.addAll(Arrays.asList(arr));
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
    // TODO Auto-generated method stub
    return null;
  }

  public int fCount(DBObject object) {
    return objects.size();
  }

}
