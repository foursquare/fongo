package com.foursquare.fongo

import org.scalatest._
import com.mongodb.{DBObject, MongoException, WriteConcern, BasicDBObject}
import collection.JavaConversions._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class FongoScalaTest extends FunSuite with BeforeAndAfter {

  var fongo: Fongo = _

  before {
    fongo = new Fongo("InMemoryMongo")
  }

  test("Fongo should not throw npe") {
    val db = fongo.getDB("myDB")
    val col = db.createCollection("myCollection", null)
    val result = col.findOne()
    assert(result == null)
  }

  test("Insert should work") {
    val collection = fongo.getDB("myDB").createCollection("myCollection", null)

    collection.insert(new BasicDBObject("basic", "basic"))

    assert(1 === collection.count())
  }

  test("MongoClient should return same DB as BB") {
    assert(fongo.getMongoClient.getDB("db") === fongo.getDB("db"))
  }

  test("MongoClient should change writeConcern") {
    val writeConcern = fongo.getMongoClient.getMongoClientOptions.getWriteConcern
    assert(fongo.getWriteConcern === writeConcern)
    assert(writeConcern != WriteConcern.FSYNC_SAFE)

    // Change write concern
    fongo.getMongoClient.setWriteConcern(WriteConcern.FSYNC_SAFE)
    assert(fongo.getWriteConcern === WriteConcern.FSYNC_SAFE)
  }

  test("UniqueIndexes should not permit create of duplicated entries") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("date", 1))
    collection.insert(new BasicDBObject("date", 2))
    val thrown = intercept[MongoException] {
      collection.insert(new BasicDBObject("date", 1))
    }
    assert(thrown.getCode === 11000)
  }

  test("UniqueIndexes should not permit update of duplicated entries when updated by _id") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))

    val thrown = intercept[MongoException] {
      collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1))
    }
    assert(thrown.getCode === 11000)

    // Verify object is NOT modify
    assert(2 === collection.find(new BasicDBObject("_id", 2)).next().get("date"))
  }

  test("UniqueIndexes can permit update of duplicated entries when updated by _id the same object.") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))

    // Test
    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 2))

    // Verify object is NOT modified
    assert(2 === collection.find(new BasicDBObject("_id", 2)).next().get("date"))
  }

  test("UniqueIndexes should not permit create of duplicated entries when updated by field") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))

    val thrown = intercept[MongoException] {
      collection.update(new BasicDBObject("date", 2), new BasicDBObject("date", 1))
    }
    assert(thrown.getCode === 11000)

    // Verify object is NOT modify
    assert(2 === collection.find(new BasicDBObject("_id", 2)).next().get("date"))
  }

  test("UniqueIndexes can permit create of duplicated entries when updated by field the same object") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))
    collection.update(new BasicDBObject("date", 1), new BasicDBObject("date", 1))

    // Verify object is NOT modify
    assert(1 === collection.find(new BasicDBObject("_id", 1)).next().get("date"))
  }

  test("UniqueIndexes should permit create of duplicated entries when index is removed") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))

    collection.dropIndex("uniqueDate")

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1))
    collection.insert(new BasicDBObject("_id", 3).append("date", 1))
  }

  test("UniqueIndexes should permit create of duplicated entries when all index are removed") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1), "uniqueDate", true)

    // Insert
    collection.insert(new BasicDBObject("_id", 1).append("date", 1))
    collection.insert(new BasicDBObject("_id", 2).append("date", 2))

    collection.dropIndex("uniqueDate")

    collection.update(new BasicDBObject("_id", 2), new BasicDBObject("date", 1))
    collection.insert(new BasicDBObject("_id", 3).append("date", 1))
  }

  test("Indexes should be removed") {
    val collection = fongo.getDB("db").createCollection("myCollection", null)

    collection.ensureIndex(new BasicDBObject("date", 1))
    collection.ensureIndex(new BasicDBObject("field", 1), "fieldIndex")
    collection.ensureIndex(new BasicDBObject("string", 1), "stringIndex", true)

    var indexInfos: List[DBObject] = collection.getIndexInfo.toList
    assert(3 === indexInfos.size)
    assert(indexInfos.exists(_.get("name").equals("date_1")))
    assert(indexInfos.map(_.get("name")).contains("fieldIndex"))
    assert(indexInfos.map(_.get("name")).contains("stringIndex"))

    collection.dropIndex("fieldIndex")
    indexInfos = collection.getIndexInfo.toList
    assert(2 === indexInfos.size)
    assert(indexInfos.map(_.get("name")).contains("date_1"))
    assert(!indexInfos.map(_.get("name")).contains("fieldIndex"))
    assert(indexInfos.map(_.get("name")).contains("stringIndex"))
  }
}