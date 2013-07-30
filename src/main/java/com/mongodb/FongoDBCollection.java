package com.mongodb;

import com.foursquare.fongo.FongoException;
import com.foursquare.fongo.impl.ExpressionParser;
import com.foursquare.fongo.impl.Filter;
import com.foursquare.fongo.impl.Index;
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
//  private final Map<Object, DBObject> objects = new LinkedHashMap<Object, DBObject>();
  private final ExpressionParser expressionParser;
  private final UpdateEngine updateEngine;
  private final boolean nonIdCollection;
  private final ExpressionParser.ObjectComparator objectComparator;
  private final Map<Set<String>, Index> indexes = new LinkedHashMap<Set<String>, Index>();
  private final Index _idIndex;

  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
    this.nonIdCollection = name.startsWith("system");
    this.expressionParser = new ExpressionParser();
    this.updateEngine = new UpdateEngine();
    this.objectComparator = expressionParser.objectComparator();
    _idIndex = new Index("_id", new BasicDBObject("_id", 1), true);
    this.indexes.put(Collections.singleton("_id"), _idIndex);
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

//      if (_idIndex.containsKey(id)) {
//        if (enforceDuplicates(concern)) {
//          throw new MongoException.DuplicateKey(fongoDb.errorResult(0, "Attempting to insert duplicate _id: " + id));
//        } else {
//          // TODO(jon) log
//        }
//      } else {
      putSizeCheck(id, obj);
//      }
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
//      if (!nonIdCollection) {
      obj.put(ID_KEY, id);
//      }
      return id;
    } else {
      return obj.get(ID_KEY);
    }
  }

  public void putSizeCheck(Object id, DBObject obj) {
    if (_idIndex.size() > 100000) {
      throw new FongoException("Whoa, hold up there.  Fongo's designed for lightweight testing.  100,000 items per collection max");
    }

    addToIndexes(obj, null);
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
      // TODO : try to find a more efficient way.
      List<DBObject> oldObjects = _idIndex.get(o);
      if (oldObjects != null && oldObjects.size() == 1) {
        removeFromIndexes(oldObjects.get(0));
      }
      fInsert(o);
      updatedDocuments++;
    } else {
      Filter filter = expressionParser.buildFilter(q);
      for (DBObject obj : filterByIndexes(q)) {
        if (filter.apply(obj)) {
          DBObject newObject = Util.clone(obj);
          updateEngine.doUpdate(newObject, o, q);
          // Check for uniqueness (throw MongoException if error)
          addToIndexes(newObject, obj);

          updatedDocuments++;
          updatedExisting = true;

          if (!multi) {
            break;
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
    int updatedDocuments = 0;
    Collection<DBObject> objectsByIndex = filterByIndexes(o);
    Filter filter = expressionParser.buildFilter(o);
    List<DBObject> ids = new ArrayList<DBObject>();
    // Double pass, objectsByIndex can be not "objects"
    for (DBObject object : objectsByIndex) {
      if (filter.apply(object)) {
        ids.add(object);
      }
    }
    // Real remove.
    for (DBObject object : ids) {
      LOG.debug("remove object : {}", object);
      removeFromIndexes(object);
      updatedDocuments++;
    }
    return new WriteResult(updateResult(updatedDocuments, false), concern);
  }

  @Override
  public synchronized void createIndex(DBObject keys, DBObject options, DBEncoder encoder) throws MongoException {
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
    List<List<Object>> notUnique = index.addAll(_idIndex.values());
    if (!notUnique.isEmpty()) {
      // Duplicate key.
      if (!enforceDuplicates(getWriteConcern())) {
        fongoDb.errorResult(11000, "E11000 duplicate key error index: " + getFullName() + "." + rec.get("name") + "  dup key: { : " + notUnique + " }").throwOnError();
      }
      return;
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
      LOG.debug("the db looks like {}", _idIndex.values());
    }

    List<DBObject> results = new ArrayList<DBObject>();

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

    Collection<DBObject> objectsFromIndex = filterByIndexes(ref);

    int seen = 0;
    Iterable<DBObject> objectsToSearch = sortObjects(orderby, objectsFromIndex);
    for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext() && foundCount <= upperLimit; ) {
      DBObject dbo = iter.next();
      if (filter.apply(dbo)) {
        if (seen++ >= numToSkip) {
          foundCount++;
          DBObject clonedDbo = Util.clone(dbo);
          if (nonIdCollection) {
            clonedDbo.removeField("_id");
          }
          results.add(clonedDbo);
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
   * @return objects from "_id" if no index found, elsewhere the restricted values from an index.
   */
  private Collection<DBObject> filterByIndexes(DBObject ref) {
    Collection<DBObject> dbObjectIterable = null;
    if (ref != null) {
      Index matchingIndex = searchIndex(ref);
      if (matchingIndex != null) {
        dbObjectIterable = matchingIndex.retrieveObjects(ref);
        if (LOG.isDebugEnabled()) {
          LOG.debug("restrict with index {}, from {} to {} elements", matchingIndex.getName(), _idIndex.size(), dbObjectIterable.size());
        }
      }
    }
    if (dbObjectIterable == null) {
      dbObjectIterable = _idIndex.values();
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
  // TODO WDEL : remove static
  public static DBObject applyProjections(DBObject result, DBObject projection) {
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

  // TODO WDEL : remove static
  private static Set<String> getProjectedFields(DBObject result, DBObject projections) {
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

  public Iterable<DBObject> sortObjects(final DBObject orderby, final Collection<DBObject> objects) {
    Iterable<DBObject> objectsToSearch = objects;
    if (orderby != null) {
      final Set<String> orderbyKeySet = orderby.keySet();
      if (!orderbyKeySet.isEmpty()) {
        List<DBObject> objectsToSort = new ArrayList(objects);

        Collections.sort(objectsToSort, new Comparator<DBObject>() {
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
        objectsToSearch = objectsToSort;
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
    for (Iterator<DBObject> iter = filterByIndexes(query).iterator(); iter.hasNext() && count <= upperLimit; ) {
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

    Iterable<DBObject> objectsToSearch = sortObjects(sort, filterByIndexes(query));
    DBObject beforeObject = null;
    DBObject afterObject = null;
    for (DBObject dbo : objectsToSearch) {
      if (filter.apply(dbo)) {
        beforeObject = dbo;
        if (!remove) {
          afterObject = Util.clone(beforeObject);
          updateEngine.doUpdate(afterObject, update, query);
          addToIndexes(afterObject, beforeObject);
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
    Set<Object> results = new LinkedHashSet<Object>();
    Filter filter = expressionParser.buildFilter(query);
    for (Iterator<DBObject> iter = filterByIndexes(query).iterator(); iter.hasNext(); ) {
      DBObject value = iter.next();
      if (filter.apply(value)) {
        List<Object> keyValues = expressionParser.getEmbeddedValues(key, value);
        for (Object keyValue : keyValues) {
          if (keyValue instanceof List) {
            results.addAll((List) keyValue);
          } else {
            results.add(keyValue);
          }
        }
      }
    }
    return new ArrayList(results);
  }

  protected synchronized void _dropIndexes(String name) throws MongoException {
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    indexColl.remove(new BasicDBObject("name", name));
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (entry.getValue().getName().equals(name)) {
        indexes.remove(entry.getKey());
        break;
      }
    }
  }

  protected synchronized void _dropIndexes() {
    List<DBObject> indexes = fongoDb.getCollection("system.indexes").find().toArray();
    // Two step for no concurrent modification exception
    for (DBObject index : indexes) {
      dropIndexes(index.get("name").toString());
    }
  }

  @Override
  public void drop() {
    _idIndex.clear();
    indexes.clear();
    fongoDb.removeCollection(this);
  }

  /**
   * Search the most restrictive index for query.
   *
   * @param query
   * @return the most restrictive index, or null.
   */
  private synchronized Index searchIndex(DBObject query) {
    Index index = null;
    int foundCommon = -1;
    Set<String> queryFields = query.keySet();
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        if (entry.getKey().size() > foundCommon || index.isUnique()) {
          index = entry.getValue();
          foundCommon = entry.getKey().size();
        }
      }
    }

    LOG.debug("searchIndex() found index {} for fields {}", index, queryFields);

    return index;
  }

  /**
   * Add entry to index.
   * If necessary, remove oldObject from index.
   *
   * @param object    new object to insert.
   * @param oldObject null if insert, old object if update.
   */
  private synchronized void addToIndexes(DBObject object, DBObject oldObject) {
    Set<String> queryFields = object.keySet();
    // First, try to see if index can add the new value.
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        List<List<Object>> error = entry.getValue().checkAddOrUpdate(object, oldObject);
        if (!error.isEmpty()) {
          // TODO formatting : E11000 duplicate key error index: test.zip.$city_1_state_1_pop_1  dup key: { : "BARRE", : "MA", : 4546.0 }
          if (!enforceDuplicates(getWriteConcern())) {
            this.fongoDb.errorResult(11000, "E11000 duplicate key error index: " + this.getFullName() + "." + entry.getValue().getName() + "  dup key : {" + error + " }").throwOnError();
          }
          return; // silently ignore.
        }
      }
    }

    Set<String> oldQueryFields = oldObject == null ? Collections.<String>emptySet() : oldObject.keySet();
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        entry.getValue().addOrUpdate(object, oldObject);
      } else if (oldQueryFields.containsAll(entry.getKey()))
        // In case of update and removing a field, we must remove from the index.
        entry.getValue().remove(oldObject);
    }
  }

  private synchronized void removeFromIndexes(DBObject object) {
    Set<String> queryFields = object.keySet();
    for (Map.Entry<Set<String>, Index> entry : indexes.entrySet()) {
      if (queryFields.containsAll(entry.getKey())) {
        entry.getValue().remove(object);
      }
    }
  }

  //@VisibleForTesting
  public synchronized Collection<Index> getIndexes() {
    return indexes.values();
  }
}
