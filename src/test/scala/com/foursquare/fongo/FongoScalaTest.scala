package com.foursquare.fongo

import org.scalatest._
import com.mongodb.{WriteConcern, BasicDBObject}

class FongoScalaTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  var fongo: Fongo = _

  before {
    fongo = new Fongo("InMemoryMongo")
  }

  "Fongo" should "not throw npe" in {
    val db = fongo.getDB("myDB")
    val col = db.createCollection("myCollection", null)
    val result = col.findOne()
    assert(result == null)
  }

  "Insert" should "work" in {
    val collection = fongo.getDB("myDB").createCollection("myCollection", null)

    collection.insert(new BasicDBObject("basic", "basic"))

    assert(1 === collection.count())
  }

  "MongoClient" should "return same DB as BB" in {
    assert(fongo.getMongoClient.getDB("db") === fongo.getDB("db"))
  }

  it should "change writeConcern" in {
    val writeConcern = fongo.getMongoClient.getMongoClientOptions.getWriteConcern
    assert(fongo.getWriteConcern === writeConcern)
    assert(writeConcern != WriteConcern.FSYNC_SAFE)

    // Change write concern
    fongo.getMongoClient.setWriteConcern(WriteConcern.FSYNC_SAFE)
    assert(fongo.getWriteConcern === WriteConcern.FSYNC_SAFE)
  }
}