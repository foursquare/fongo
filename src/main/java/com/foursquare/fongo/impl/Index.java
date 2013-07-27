package com.foursquare.fongo.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An index for the MongoDB.
 * Must be immutable.
 */
public class Index {
  private final String name;
  private final List<String> fields;
  private final boolean unique;
  // Contains all "_id" than field value can have.
  private final ConcurrentHashMap<List<List<Object>>, Set<Object>> mapValues = new ConcurrentHashMap<List<List<Object>>, Set<Object>>();

  public Index(String name, List<String> fields, boolean unique) {
    this.name = name;
    this.fields = fields;
    this.unique = unique;
  }

  public Index(String name, DBObject keys, boolean unique) {
    this(name, new ArrayList<String>(keys.keySet()), unique);
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  public List<String> getFields() {
    return fields;
  }

  /**
   * @param object
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> add(Object id, DBObject object) {
    List<List<Object>> fieldsForIndex = IndexUtil.INSTANCE.extractFields(object, getFields());
//    DBObject id = new BasicDBObject(FongoDBCollection.ID_KEY, object.get(FongoDBCollection.ID_KEY));
    if (unique) {
      // Return null only if key was absent.
      if (mapValues.putIfAbsent(fieldsForIndex, Collections.singleton(id)) != null) {
        return fieldsForIndex;
      }
    } else {
      // Extract previous values
      Set<Object> values = mapValues.get(fieldsForIndex);
      if (values == null) {
        // Create if absent.
        values = new HashSet<Object>();
        mapValues.put(fieldsForIndex, values);
      }

      // Add to values.
      values.add(id);
    }
    return Collections.emptyList();
  }

  /**
   * Check, in case of unique index, if we can add it.
   *
   * @param object
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> checkAdd(DBObject object) {
    if (unique) {
      List<List<Object>> fieldsForIndex = IndexUtil.INSTANCE.extractFields(object, getFields());
      if (mapValues.containsKey(fieldsForIndex)) {
        return fieldsForIndex;
      }
    }
    return Collections.emptyList();
  }

  /**
   * @param object
   */
  public synchronized void remove(Object id, DBObject object) {
    List<List<Object>> fieldsForIndex = IndexUtil.INSTANCE.extractFields(object, getFields());
//    DBObject id = new BasicDBObject(FongoDBCollection.ID_KEY, object.get(FongoDBCollection.ID_KEY));
    if (unique) {
      // Return null only if key was absent.
      mapValues.remove(fieldsForIndex);
    } else {
      // Extract previous values
      Set<Object> values = mapValues.get(fieldsForIndex);
      if (values != null) {
        // Create if absent.
        values.remove(id);
      }
    }
  }

  /**
   * Multiple add of objects.
   *
   * @param objectsById <_id, value>
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> addAll(Set<Map.Entry<Object, DBObject>> objectsById) {
    for (Map.Entry<Object, DBObject> entry : objectsById) {
      List<List<Object>> nonUnique = add(entry.getKey(), entry.getValue());
      if (!nonUnique.isEmpty()) {
        return nonUnique;
      }
    }
    return Collections.emptyList();
  }

  public synchronized Collection<DBObject> getIds() {

    return Collections.emptyList();
  }
}
