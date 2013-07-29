package com.foursquare.fongo

import _root_.com.mongodb.{MongoException, DBObject, WriteConcern, BasicDBObject}
import org.scalatest._
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

  ignore("MongoClient should change writeConcern") {
    val writeConcern = fongo.getMongoClient.getMongoClientOptions.getWriteConcern
    assert(fongo.getWriteConcern === writeConcern)
    assert(writeConcern != WriteConcern.FSYNC_SAFE)

    // Change write concern
    fongo.getMongoClient.setWriteConcern(WriteConcern.FSYNC_SAFE)
    assert(fongo.getWriteConcern === WriteConcern.FSYNC_SAFE)
  }

}