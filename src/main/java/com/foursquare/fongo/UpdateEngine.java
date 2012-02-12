package com.foursquare.fongo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UpdateEngine {

  ExpressionParser expressionParser = new ExpressionParser();

  void keyCheck(String key, Set<String> seenKeys) {
    if (!seenKeys.add(key)){
      throw new FongoException("attempting more than one atomic update on on " + key);
    }
  }
  
  abstract class BasicUpdate {
    final String command;

    public BasicUpdate(String command) {
      this.command = command;
    }
    
    public DBObject doUpdate(DBObject obj, DBObject update, Set<String> seenKeys){
      DBObject updateObject = (DBObject) update.get(command);
      for (String updateKey : updateObject.keySet()) {
        keyCheck(updateKey, seenKeys);
        doSingleKeyUpdate(updateKey, obj, updateObject.get(updateKey));
      }
      return obj;
    }

    abstract void doSingleKeyUpdate(String updateKey, DBObject obj, Object object);
  }
  
  abstract class BasicMergeUpdate extends BasicUpdate {

    public BasicMergeUpdate(String command) {
      super(command);
    }
     
    abstract void mergeAction(String subKey, DBObject subObject, Object object);
    
    @Override
    void doSingleKeyUpdate(final String updateKey, final DBObject objOriginal, Object object) {
      String[] path = ExpressionParser.DOT_PATTERN.split(updateKey);
      String subKey = path[0];
      
      DBObject obj = objOriginal;
      for (int i = 0; i < path.length - 1; i++){
        if (!obj.containsField(subKey)){
          obj.put(subKey, new BasicDBObject());
        }
        Object value = obj.get(subKey);
        if (value instanceof DBObject){
          obj = (DBObject) value;
        } else {
          throw new FongoException("subfield must be object. " + updateKey + " not in " + objOriginal);
        }
        subKey = path[i + 1];
      }
      mergeAction(subKey, obj, object);
    }
  }
  
  Number genericAdd(Number left, Number right) { 
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() + (right.doubleValue());
    } else if  (left instanceof Integer) {
      return left.intValue() + (right.intValue());
    } else {
      return left.longValue() + (right.intValue());
    }
  }
  
  final List<BasicUpdate> commands = Arrays.<BasicUpdate>asList(
      new BasicMergeUpdate("$set") {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object) {
          subObject.put(subKey, object);
        }
      },
      new BasicMergeUpdate("$inc") {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object) {
          Number updateNumber = expressionParser.typecast(command + " value", object, Number.class);
          Object oldValue = subObject.get(subKey);
          if (oldValue == null){
            subObject.put(subKey, updateNumber);
          } else {
            Number oldNumber = expressionParser.typecast(subKey + " value", oldValue, Number.class);
            subObject.put(subKey, genericAdd(oldNumber, updateNumber));
          }
        }
      },
      new BasicUpdate("$unset") {
        @Override
        void doSingleKeyUpdate(final String updateKey, final DBObject objOriginal, Object object) {
          String[] path = ExpressionParser.DOT_PATTERN.split(updateKey);
          String subKey = path[0];
          
          DBObject obj = objOriginal;
          for (int i = 0; i < path.length - 1; i++){
            if (!obj.containsField(subKey)){
              return;
            }
            Object value = obj.get(subKey);
            if (value instanceof DBObject){
              obj = (DBObject) value;
            } else {
              throw new FongoException("subfield must be object. " + updateKey + " not in " + objOriginal);
            }
            subKey = path[i + 1];
          }
          obj.removeField(subKey);
        }
      }
  );
  final Map<String, BasicUpdate> commandMap = createCommandMap();
  private Map<String, BasicUpdate> createCommandMap() {
    Map<String, BasicUpdate> map = new HashMap<String, BasicUpdate>();
    for (BasicUpdate item : commands){
      map.put(item.command, item);
    }
    return map;
  }
  
  public DBObject doUpdate(final DBObject obj, final DBObject update) {
    boolean updateDone = false;
    Set<String> seenKeys = new HashSet<String>();
    for (String command : update.keySet()) {
      BasicUpdate basicUpdate = commandMap.get(command);
      if (basicUpdate != null){
        basicUpdate.doUpdate(obj, update, seenKeys);
        updateDone = true;
      } else if (command.startsWith("$")){
        throw new FongoException("usupported update: " + update);
      }
    }
    if (!updateDone){
      for (Iterator<String> iter = obj.keySet().iterator(); iter.hasNext();) {
        String key = iter.next();
        if (key != "_id"){
          iter.remove();
        }
      }
      obj.putAll(update);
    }
    return obj;
  }





}
