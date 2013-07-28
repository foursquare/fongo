package com.mongodb;

import com.foursquare.fongo.FongoException;
import com.foursquare.fongo.impl.ExpressionParser;
import com.foursquare.fongo.impl.Filter;
import com.foursquare.fongo.impl.Index;
import com.foursquare.fongo.impl.IndexUtil;
import com.foursquare.fongo.impl.UpdateEngine;
import com.foursquare.fongo.impl.Util;
import org.bson.BSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * fongo override of com.mongodb.DBCollection
 * you shouldn't need to use this class directly
 *
 * @author jon
 */
public class FongoDBCollection extends DBCollection {
  final static Logger LOG = LoggerFactory.getLogger(FongoDBCollection.class);

  public static final String ID_KEY = "_id";
  private final FongoDB fongoDb;
  // LinkedHashMap maintains insertion order
  // TODO(jon) separate _id index from storage
  private final Map<Object, DBObject> objects = new LinkedHashMap<Object, DBObject>();
  private final ExpressionParser expressionParser;
  private final UpdateEngine updateEngine;
  private final boolean nonIdCollection;
  private final ObjectComparator objectComparator;
  private final Map<Set<String>, Index> indexes = new LinkedHashMap<Set<String>, Index>();

  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
    this.nonIdCollection = name.startsWith("system");
    this.expressionParser = new ExpressionParser();
    this.updateEngine = new UpdateEngine();
    this.objectComparator = new ObjectComparator();
  }

  class ObjectComparator implements Comparator {
    @Override
    public int compare(Object o1, Object o2) {
      return expressionParser.compareObjects(o1, o2);
    }
  }

  private CommandResult insertResult(int updateCount) {
    CommandResult result = fongoDb.okResult();
    result.put("n", updateCount);
    return result;
  }

  private CommandResult updateResult(int updateCount, boolean updatedExisting) {
    CommandResult result = fongoDb.okResult();
    result.put("n", updateCount);
    result.put("updatedExisting", updatedExisting);
    return result;
  }

  @Override
  public synchronized WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    return insert(Arrays.asList(arr), concern, encoder);
  }

  @Override
  public synchronized WriteResult insert(List<DBObject> toInsert, WriteConcern concern, DBEncoder encoder) {
    for (DBObject obj : toInsert) {
      filterLists(obj);
      if (LOG.isDebugEnabled()) {
        LOG.debug("insert: " + obj);
      }
      Object id = putIdIfNotPresent(obj);

      if (objects.containsKey(id)) {
        if (enforceDuplicates(concern)) {
          throw new MongoException.DuplicateKey(fongoDb.errorResult(0, "Attempting to insert duplicate _id: " + id));
        } else {
          // TODO(jon) log          
        }
      } else {
        putSizeCheck(id, obj);
      }
    }
    return new WriteResult(insertResult(toInsert.size()), concern);
  }

  boolean enforceDuplicates(WriteConcern concern) {
    return !(WriteConcern.NONE.equals(concern) || WriteConcern.NORMAL.equals(concern));
  }

  public Object putIdIfNotPresent(DBObject obj) {
    if (obj.get(ID_KEY) == null) {
      ObjectId id = new ObjectId();
      id.notNew();
      if (!nonIdCollection) {
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

    // TODO WDEL refactor
    addToIndexes(obj);

    objects.put(id, obj);
  }

  public DBObject filterLists(DBObject dbo) {
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
    Object replacementValue = BSON.applyEncodingHooks(value);
    if (replacementValue instanceof DBObject) {
      replacementValue = filterLists((DBObject) replacementValue);
    } else if (replacementValue instanceof List) {
      BasicDBList list = new BasicDBList();
      for (Object listItem : (List) replacementValue) {
        list.add(replaceListAndMap(listItem));
      }
      replacementValue = list;
    } else if (replacementValue instanceof Object[]) {
      BasicDBList list = new BasicDBList();
      for (Object listItem : (Object[]) replacementValue) {
        list.add(replaceListAndMap(listItem));
      }
      replacementValue = list;
    } else if (replacementValue instanceof Map) {
      BasicDBObject newDbo = new BasicDBObject();
      for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) ((Map) replacementValue).entrySet()) {
        newDbo.put(entry.getKey(), replaceListAndMap(entry.getValue()));
      }
      replacementValue = newDbo;
    }
    return replacementValue;
  }


  protected void fInsert(DBObject obj) {
    Object id = putIdIfNotPresent(obj);
    putSizeCheck(id, obj);
  }


  @Override
  public synchronized WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
                                         DBEncoder encoder) throws MongoException {

    filterLists(q);
    filterLists(o);

    if (LOG.isDebugEnabled()) {
      LOG.debug("update(" + q + ", " + o + ", " + upsert + ", " + multi + ")");
    }

    if (o.containsField(ID_KEY) && q.containsField(ID_KEY) && !o.get(ID_KEY).equals(q.get(ID_KEY))) {
      throw new MongoException.DuplicateKey(fongoDb.errorResult(0, "can not change _id of a document " + ID_KEY));
    }

    int updatedDocuments = 0;
    boolean idOnlyUpdate = q.containsField(ID_KEY) && q.keySet().size() == 1;
    boolean updatedExisting = false;

    if (idOnlyUpdate && isNotUpdateCommand(o)) {
      if (!o.containsField(ID_KEY)) {
        o.put(ID_KEY, q.get(ID_KEY));
      }
      List<DBObject> futureObjects = new ArrayList<DBObject>(objects.values());
      Object id = o.get(ID_KEY);
      Object toModify = objects.get(id);
      if (toModify != null) {
        futureObjects.remove(toModify);
      }
      futureObjects.add(o);
      checkForUniqueness(futureObjects);
      fInsert(o);
      updatedDocuments++;
    } else {
      List idsIn = idsIn(q);
      List<DBObject> futureObjects = new ArrayList<DBObject>(objects.values());
      if (idOnlyUpdate && idsIn.size() > 0) {
        for (Object id : idsIn) {
          DBObject existingObject = objects.get(id);
          if (existingObject != null) {
            futureObjects.remove(existingObject);
            DBObject newObject = Util.clone(existingObject);
            updateEngine.doUpdate(newObject, o, q);
            // Check for uniqueness
            futureObjects.add(newObject);
            checkForUniqueness(futureObjects);

            // Set object now.
            objects.put(id, newObject);
            updatedDocuments++;
            updatedExisting = true;

            if (!multi) {
              break;
            }
          }
        }
      } else {
        Filter filter = expressionParser.buildFilter(q);
        for (DBObject obj : filterByIndexes(q, objects.values())) {
          if (filter.apply(obj)) {
            futureObjects.remove(obj);
            DBObject newObject = Util.clone(obj);
            updateEngine.doUpdate(newObject, o, q);
            // Check for uniqueness
            futureObjects.add(newObject);
            checkForUniqueness(futureObjects);

            // Set for real now.
            updateEngine.doUpdate(obj, o, q);
            updatedDocuments++;
            updatedExisting = true;

            if (!multi) {
              break;
            }
          }
        }
      }
      if (updatedDocuments == 0 && upsert) {
        BasicDBObject newObject = createUpsertObject(q);
        fInsert(updateEngine.doUpdate(newObject, o, q));
      }
    }
    return new WriteResult(updateResult(updatedDocuments, updatedExisting), concern);
  }


  public List idsIn(DBObject query) {
    Object idValue = query.get(ID_KEY);
    if (idValue == null || query.keySet().size() > 1) {
      return Collections.emptyList();
    } else if (idValue instanceof DBObject) {
      DBObject idDbObject = (DBObject) idValue;
      List inList = (List) idDbObject.get(ExpressionParser.IN);

      // I think sorting the inputed keys is a rough
      // approximation of how mongo creates the bounds for walking
      // the index.  It has the desired affect of returning results
      // in _id index order, but feels pretty hacky.
      if (inList != null) {
        Object[] inListArray = inList.toArray(new Object[inList.size()]);
        // ids could be DBObjects, so we need a comparator that can handle that
        Arrays.sort(inListArray, objectComparator);
        return Arrays.asList(inListArray);
      }
      if (!isNotUpdateCommand(idValue)) {
        return Collections.emptyList();
      }
    }
    return Collections.singletonList(idValue);
  }

  protected BasicDBObject createUpsertObject(DBObject q) {
    BasicDBObject newObject = new BasicDBObject();
    List idsIn = idsIn(q);

    if (!idsIn.isEmpty()) {
      newObject.put(ID_KEY, idsIn.get(0));
    } else {
      BasicDBObject filteredQuery = new BasicDBObject();
      for (String key : q.keySet()) {
        Object value = q.get(key);
        if (isNotUpdateCommand(value)) {
          filteredQuery.put(key, value);
        }
      }
      updateEngine.mergeEmbeddedValueFromQuery(newObject, filteredQuery);
    }
    return newObject;
  }

  public boolean isNotUpdateCommand(Object value) {
    boolean okValue = true;
    if (value instanceof DBObject) {
      for (String innerKey : ((DBObject) value).keySet()) {
        if (innerKey.startsWith("$")) {
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
    filterLists(o);
    if (LOG.isDebugEnabled()) {
      LOG.debug("remove: " + o);
    }
    List idList = idsIn(o);

    int updatedDocuments = 0;
    if (!idList.isEmpty()) {
      for (Object id : idList) {
        DBObject object = objects.remove(id);
        removeFromIndexes(object);
      }
      updatedDocuments = idList.size();
    } else {
      Collection<DBObject> objectsValues = objects.values();
      Collection<DBObject> objectsByIndex = filterByIndexes(o, objectsValues);
      Filter filter = expressionParser.buildFilter(o);
      if (nonIdCollection || objectsValues == objectsByIndex) {
        for (Iterator<DBObject> iter = objectsByIndex.iterator(); iter.hasNext(); ) {
          DBObject dbo = iter.next();
          if (filter.apply(dbo)) {
            iter.remove();
            removeFromIndexes(dbo);
            updatedDocuments++;
          }
        }
      } else {
        List<Object> ids = new ArrayList<Object>();
        // Double pass, objectsByIndex can be not "objects"
        for (DBObject object : objectsByIndex) {
          if (filter.apply(object)) {
            ids.add(object.get(ID_KEY));
          }
        }
        // Real remove.
        for (Object id : ids) {
          DBObject object = objects.remove(id);
          LOG.debug("remove id {}, object : {}", id, object);
          removeFromIndexes(object);
          updatedDocuments++;
        }
      }
    }
    return new WriteResult(updateResult(updatedDocuments, false), concern);
  }

  @Override
  public void createIndex(DBObject keys, DBObject options, DBEncoder encoder) throws MongoException {
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    BasicDBObject rec = new BasicDBObject();
    rec.append("v", 1);
    rec.append("key", keys);
    rec.append("ns", this.getDB().getName() + "." + this.getName());
    if (options != null && !options.containsField("name")) {
      StringBuilder sb = new StringBuilder();
      boolean firstLoop = true;
      for (String keyName : keys.keySet()) {
        if (!firstLoop) {
          sb.append("_");
        }
        sb.append(keyName).append("_").append(keys.get(keyName));
        firstLoop = false;
      }
      rec.append("name", sb.toString());
    } else {
      rec.append("name", options.get("name"));
    }
    // Ensure index doesn't exist.
    if (indexColl.findOne(rec) != null) {
      return;
    }

    // Unique index must not be in previous find.
    boolean unique = options != null && Boolean.TRUE.equals(options.get("unique"));
    if (unique) {
      rec.append("unique", unique);
    }
    rec.putAll(options);

    Index index = new Index((String) rec.get("name"), keys, unique);
    List<List<Object>> notUnique = index.addAll(objects.entrySet());
    if (!notUnique.isEmpty()) {
      // Duplicate key.
      fongoDb.errorResult(11000, "E11000 duplicate key error index: " + getFullName() + "." + rec.get("name") + "  dup key: { : " + notUnique + " }").throwOnError();
    }
    indexes.put(new HashSet<String>(index.getFields()), index);

    // Add index if all fine.
    indexColl.insert(rec);
  }

  @Override
  public DBObject findOne(DBObject ref, DBObject fields, DBObject orderBy, ReadPreference readPref) {
    Iterator<DBObject> resultIterator = __find(ref, fields, 0, 1, -1, 0, readPref, null);
    return resultIterator.hasNext() ? resultIterator.next() : null;
  }

  /**
   * note: encoder, decoder, readPref, options are ignored
   */
  @Override
  Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                            ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {
    return __find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder);
  }

  /**
   * note: decoder, readPref, options are ignored
   */
  @Override
  synchronized Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                                         ReadPreference readPref, DBDecoder decoder) throws MongoException {
    filterLists(ref);
    if (LOG.isDebugEnabled()) {
      LOG.debug("find({}, {}).skip({}).limit({})", new Object[]{ref, fields, numToSkip, limit,});
      LOG.debug("the db looks like {}", objects);
    }

    List idList = idsIn(ref);
    List<DBObject> results = new ArrayList<DBObject>();

    if (!idList.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("find using id index only {}", idList);
      }
      for (Object id : idList) {
        DBObject result = objects.get(id);
        if (result != null) {
          results.add(result);
        }
      }
    } else {
      DBObject orderby = null;
      if (ref.containsField("$query") && ref.containsField("$orderby")) {
        orderby = (DBObject) ref.get("$orderby");
        ref = (DBObject) ref.get("$query");
      }

      Filter filter = expressionParser.buildFilter(ref);
      int foundCount = 0;
      int upperLimit = Integer.MAX_VALUE;
      if (limit > 0) {
        upperLimit = limit;
      }

      Collection<DBObject> objectsFromIndex = filterByIndexes(ref, objects.values());

      int seen = 0;
      Collection<DBObject> objectsToSearch = sortObjects(orderby, objectsFromIndex);
      for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext() && foundCount <= upperLimit; ) {
        DBObject dbo = iter.next();
        if (filter.apply(dbo)) {
          if (seen++ >= numToSkip) {
            foundCount++;
            results.add(dbo);
          }
        }
      }
    }

    if (fields != null && !fields.keySet().isEmpty()) {
      LOG.info("applying projections {}", fields);
      results = applyProjections(results, fields);
    } else {
      results = copyResults(results);
    }

    LOG.debug("found results {}", results);

    return results.iterator();
  }

  /**
   * Return "objects.values()" if no index found.
   *
   * @param ref
   * @param objectsValues
   * @return objectsValues if no index found, elsewhere the restricted values from an index.
   */
  private Collection<DBObject> filterByIndexes(DBObject ref, Collection<DBObject> objectsValues) {
    Collection<DBObject> dbObjectIterable = objectsValues;
    if (ref != null) {
      Index matchingIndex = searchIndex(ref);
      if (matchingIndex != null) {
        dbObjectIterable = matchingIndex.retrieveObjects(ref);
        LOG.info("restrict with index {}, from {} to {} elements", matchingIndex.getName(), objectsValues.size(), dbObjectIterable.size());
      }
    }
    return dbObjectIterable;
  }

  private List<DBObject> copyResults(final List<DBObject> results) {
    final List<DBObject> ret = new ArrayList<DBObject>(results.size());

    for (DBObject result : results) {
      ret.add(Util.clone(result));
    }

    return ret;
  }

  private List<DBObject> applyProjections(List<DBObject> results, DBObject projection) {
    final List<DBObject> ret = new ArrayList<DBObject>(results.size());

    for (DBObject result : results) {
      ret.add(applyProjections(result, projection));
    }

    return ret;
  }

  /**
   * Applies the requested <a href=
   * "http://docs.mongodb.org/manual/core/read-operations/#result-projections"
   * >projections</a> to the given object.
   */
  protected DBObject applyProjections(DBObject result, DBObject projection) {
    Set<String> projectedFields = getProjectedFields(result, projection);
    LOG.debug("fields after projection of {}: {}", projection, projectedFields);

    BasicDBObject ret = new BasicDBObject();

    for (String field : projectedFields) {
      Object value = result.get(field);

      if (value instanceof DBObject) {
        value = Util.clone((DBObject) value);
      }

      ret.append(field, value);
    }

    return ret;
  }

  private Set<String> getProjectedFields(DBObject result, DBObject projections) {
    Set<String> includedFields = new HashSet<String>();
    Set<String> excludedFields = new HashSet<String>();

    for (String field : projections.keySet()) {
      boolean included = ((Number) projections.get(field)).intValue() > 0;

      if (included) {
        includedFields.add(field);
      } else {
        excludedFields.add(field);
      }
    }

    boolean including = !includedFields.isEmpty();
    boolean excluding = excludedFields.size() > (excludedFields.contains(ID_KEY) ? 1 : 0);

    if (including && excluding) {
      throw new IllegalArgumentException(
          "You cannot combine inclusion and exclusion semantics in a single projection with the exception of the _id field: "
              + projections);
    }

    // the _id is always returned unless explicitly excluded
    if (including && !excludedFields.contains(ID_KEY)) {
      includedFields.add(ID_KEY);
    }

    Set<String> fieldsToRetain = new HashSet<String>(result.keySet());
    if (including) {
      fieldsToRetain.retainAll(includedFields);
    } else {
      fieldsToRetain.removeAll(excludedFields);
    }

    return fieldsToRetain;
  }

  public Collection<DBObject> sortObjects(final DBObject orderby, final Collection<DBObject> objects) {
    Collection<DBObject> objectsToSearch = objects;
    if (orderby != null) {
      final Set<String> orderbyKeySet = orderby.keySet();
      if (!orderbyKeySet.isEmpty()) {
        DBObject[] objectsToSort = objects.toArray(new DBObject[objects.size()]);

        Arrays.sort(objectsToSort, new Comparator<DBObject>() {
          @Override
          public int compare(DBObject o1, DBObject o2) {
            for (String sortKey : orderbyKeySet) {
              final List<String> path = Util.split(sortKey);
              int sortDirection = (Integer) orderby.get(sortKey);

              List<Object> o1list = expressionParser.getEmbeddedValues(path, o1);
              List<Object> o2list = expressionParser.getEmbeddedValues(path, o2);

              int compareValue = expressionParser.compareLists(o1list, o2list) * sortDirection;
              if (compareValue != 0) {
                return compareValue;
              }
            }
            return 0;
          }
        });
        objectsToSearch = Arrays.asList(objectsToSort);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("sorted objectsToSearch " + objectsToSearch);
    }
    return objectsToSearch;
  }


  @Override
  public synchronized long getCount(DBObject query, DBObject fields, long limit, long skip) {
    filterLists(query);
    Filter filter = query == null ? ExpressionParser.AllFilter : expressionParser.buildFilter(query);
    long count = 0;
    long upperLimit = Long.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }
    int seen = 0;
    for (Iterator<DBObject> iter = filterByIndexes(query, objects.values()).iterator(); iter.hasNext() && count <= upperLimit; ) {
      DBObject value = iter.next();
      if (filter.apply(value)) {
        if (seen++ >= skip) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public synchronized long getCount(DBObject query, DBObject fields, ReadPreference readPrefs) {
    //as we're in memory we don't need to worry about readPrefs
    return getCount(query, fields, 0, 0);
  }

  @Override
  public synchronized DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert) {
    filterLists(query);
    filterLists(update);
    Filter filter = expressionParser.buildFilter(query);

    Collection<DBObject> objectsToSearch = sortObjects(sort, objects.values());
    DBObject beforeObject = null;
    DBObject afterObject = null;
    for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext(); ) {
      DBObject dbo = iter.next();
      if (filter.apply(dbo)) {
        beforeObject = dbo;
        if (!remove) {
          afterObject = Util.clone(beforeObject);
          fInsert(updateEngine.doUpdate(afterObject, update, query));
        } else {
          remove(dbo);
          return dbo;
        }
      }
    }
    if (beforeObject != null && !returnNew) {
      return beforeObject;
    }
    if (beforeObject == null && upsert && !remove) {
      beforeObject = new BasicDBObject();
      afterObject = createUpsertObject(query);
      fInsert(updateEngine.doUpdate(afterObject, update, query));
    }
    if (returnNew) {
      return Util.clone(afterObject);
    } else {
      return Util.clone(beforeObject);
    }
  }

  @Override
  public synchronized List distinct(String key, DBObject query) {
    filterLists(query);
    List<Object> results = new ArrayList<Object>();
    Filter filter = expressionParser.buildFilter(query);
    for (Iterator<DBObject> iter = filterByIndexes(query, objects.values()).iterator(); iter.hasNext(); ) {
      DBObject value = iter.next();
      if (filter.apply(value) && !results.contains(value.get(key))) {
        results.add(value.get(key));
      }
    }
    return results;
  }

  protected void _dropIndexes(String name) throws MongoException {
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    indexColl.remove(new BasicDBObject("name", name));
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (entry.getValue().getName().equals(name)) {
        indexes.remove(entry.getKey());
        break;
      }
    }
  }

  protected void _dropIndexes() {
    List<DBObject> indexes = fongoDb.getCollection("system.indexes").find().toArray();
    // Two step for no concurrent modification exception
    for (DBObject index : indexes) {
      dropIndexes(index.get("name").toString());
    }
  }

  @Override
  public void drop() {
    objects.clear();
    fongoDb.removeCollection(this);
  }

  /**
   * @param futureObjects
   * @throws MongoException in case of duplicate entry in index.
   */
  private void checkForUniqueness(List<DBObject> futureObjects) throws MongoException {
    Index index = IndexUtil.INSTANCE.checkForUniqueness(indexes.values(), futureObjects);
    if (index != null) {
//E11000 duplicate key error index: test.zip.$city_1_state_1_pop_1  dup key: { : "AGAWAM", : "MA", : 15338.0 }
      // TODO
      throw new MongoException.DuplicateKey(fongoDb.errorResult(11000, "Attempting to insert duplicate on index: " + index.getName()));
    }
  }

  /**
   * Search the most restrictive index for query.
   *
   * @param query
   * @return the most restrictive index, or null.
   */
  private Index searchIndex(DBObject query) {
    Index index = null;
    int foundCommon = -1;
    Set<String> queryFields = query.keySet();
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        if (entry.getKey().size() > foundCommon) {
          index = entry.getValue();
          foundCommon = entry.getKey().size();
        }
      }
    }

    LOG.debug("searchIndex() found index {} for fields {}", index, queryFields);

    return index;
  }

  private void addToIndexes(DBObject object) {
    Set<String> queryFields = object.keySet();
    // First, try to see if index can add the new value.
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        entry.getValue().checkAdd(object);
      }
    }
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        entry.getValue().add(object);
      }
    }
  }

  private void removeFromIndexes(DBObject object) {
    Set<String> queryFields = object.keySet();
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        entry.getValue().remove(object);
      }
    }
  }

  //@VisibleForTesting
  public Collection<Index> getIndexes() {
    return indexes.values();
  }
}
