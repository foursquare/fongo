package com.foursquare.fongo.impl;

import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * An index for the MongoDB.
 */
public class Index {
  private final String name;
  private final DBObject keys;
  private final Set<String> fields;
  private final boolean unique;
  private final ExpressionParser expressionParser = new ExpressionParser();
  private final ExpressionParser.ObjectComparator objectComparator;
  // Contains all dbObject than field value can have
  private final Map<DBObject, List<DBObject>> mapValues;
  private int lookupCount = 0;

  public Index(String name, DBObject keys, boolean unique, boolean insertOrder) {
    this.name = name;
    this.fields = Collections.unmodifiableSet(keys.keySet()); // Setup BEFORE keys.
    this.keys = exludeIdIfNecessary(keys);
    this.unique = unique;

    this.objectComparator = expressionParser.buildObjectComparator(isAsc(keys));
    if (insertOrder) {
      this.mapValues = new LinkedHashMap<DBObject, List<DBObject>>();
    } else {
      this.mapValues = new TreeMap<DBObject, List<DBObject>>(objectComparator);
    }
  }

  private DBObject exludeIdIfNecessary(DBObject keys) {
    DBObject nKeys = Util.clone(keys);
    if (!nKeys.containsField(FongoDBCollection.ID_KEY)) {
      // Remove _id for projection.
      nKeys.put("_id", 0);
    }
    return nKeys;
  }

  private boolean isAsc(DBObject keys) {
    return ((Comparable) keys.toMap().values().iterator().next()).compareTo(0) == 1;
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  public DBObject getKeys() {
    return keys;
  }

  public Set<String> getFields() {
    return fields;
  }

  /**
   * @param object    new object to insert in the index.
   * @param oldObject in update, old objet to remove from index.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> addOrUpdate(DBObject object, DBObject oldObject) {
    if (oldObject != null) {
      this.remove(oldObject); // TODO : optim ?
    }

    DBObject key = getKeyFor(object);

    if (unique) {
      // Unique must check if he's really unique.
      if (mapValues.containsKey(key)) {
        return extractFields(object, key.keySet());
      }
      mapValues.put(key, Collections.singletonList(object)); // DO NOT CLONE !
    } else {
      // Extract previous values
      List<DBObject> values = mapValues.get(key);
      if (values == null) {
        // Create if absent.
        values = new ArrayList<DBObject>();
        mapValues.put(key, values);
      }

      // Add to values.
      values.add(object); // DO NOT CLONE ! Indexes must share the same object.
    }
    return Collections.emptyList();
  }

  /**
   * Check, in case of unique index, if we can add it.
   *
   * @param object
   * @param oldObject old object if update, null elsewhere.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> checkAddOrUpdate(DBObject object, DBObject oldObject) {
    if (unique) {
      DBObject key = getKeyFor(object);
      List<DBObject> objects = mapValues.get(key);
      if (objects != null && !objects.contains(oldObject)) {
        List<List<Object>> fieldsForIndex = extractFields(object, getFields());
        return fieldsForIndex;
      }
    }
    return Collections.emptyList();
  }

  /**
   * Create the key for the hashmap.
   *
   * @param object
   * @return
   */
  private synchronized DBObject getKeyFor(DBObject object) {
    DBObject applyProjections = FongoDBCollection.applyProjections(object, keys);
    return applyProjections;
  }

  /**
   * Remove an object from the index.
   *
   * @param object to remove from the index.
   */
  public synchronized void remove(DBObject object) {
    DBObject key = getKeyFor(object);
    // Extract previous values
    List<DBObject> values = mapValues.get(key);
    if (values != null) {
      // Last entry ? or uniqueness ?
      if (values.size() == 1) {
        mapValues.remove(key);
      } else {
        values.remove(object);
      }
    }
  }

  /**
   * Multiple add of objects.
   *
   * @param objects to add.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public synchronized List<List<Object>> addAll(Iterable<DBObject> objects) {
    for (DBObject object : objects) {
      List<List<Object>> nonUnique = addOrUpdate(object, null);
      // TODO(twillouer) : must handle writeConcern.
      if (!nonUnique.isEmpty()) {
        return nonUnique;
      }
    }
    return Collections.emptyList();
  }

  // Only for unique index and for query with values. ($in doens't work by example.)
  public synchronized List<DBObject> get(DBObject query) {
    if(!unique) {
      throw new IllegalStateException("get is only for unique index");
    }
    lookupCount++;

    DBObject key = getKeyFor(query);
    return mapValues.get(key);
  }

  public synchronized Collection<DBObject> retrieveObjects(DBObject query) {
    // Optimization
    if(unique && query.keySet().size() == 1 && !(query.toMap().values().iterator().next() instanceof DBObject)) {
      return get(query);
    }

    lookupCount++;

    // Filter for the key.
    Filter filterKey = expressionParser.buildFilter(query, getFields());
    // Filter for the data.
    Filter filter = expressionParser.buildFilter(query);
    List<DBObject> result = new ArrayList<DBObject>();
    for (Map.Entry<DBObject, List<DBObject>> entry : mapValues.entrySet()) {
      if (filterKey.apply(entry.getKey())) {
        for (DBObject object : entry.getValue()) {
          if (filter.apply(object)) {
            result.add(object); // DO NOT CLONE ! need for update.
          }
        }
      }
    }
    return result;
  }

  public long getLookupCount() {
    return lookupCount;
  }

  @Override
  public String toString() {
    return "Index{" +
        "name='" + name + '\'' +
        '}';
  }

  public synchronized int size() {
    int size = 0;
    if (unique) {
      size = mapValues.size();
    } else {
      for (Map.Entry<DBObject, List<DBObject>> entry : mapValues.entrySet()) {
        size += entry.getValue().size();
      }
    }
    return size;
  }

  public synchronized List<DBObject> values() {
    List<DBObject> values = new ArrayList<DBObject>(mapValues.size() * 10);
    for (List<DBObject> objects : mapValues.values()) {
      values.addAll(objects);
    }
    return values;
  }

  public synchronized void clear() {
    mapValues.clear();
  }

  private List<List<Object>> extractFields(DBObject dbObject, Collection<String> fields) {
    List<List<Object>> fieldValue = new ArrayList<List<Object>>();
    for (String field : fields) {
      List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
      fieldValue.add(embeddedValues);
    }
    return fieldValue;
  }

  /**
   * Return true if index can handle this query.
   *
   * @param queryFields fields of the query.
   * @return true if index can be used.
   */
  public boolean canHandle(Set<String> queryFields) {
    return queryFields.containsAll(fields);
  }
}
