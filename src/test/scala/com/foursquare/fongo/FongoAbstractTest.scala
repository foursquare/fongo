package com.foursquare.fongo

import _root_.com.mongodb.{MongoClient, DBCollection}
import org.scalatest._
import java.util.UUID

trait FongoAbstractTest extends FunSuite with BeforeAndAfter {
  // If you want to test against real world (a real mongodb client).
  def realWorld: Boolean

  def init(): Unit = {}

  var collection: DBCollection = _

  before {
    if (realWorld) {
      val mongo = new MongoClient("localhost")
      collection = mongo.getDB(UUID.randomUUID().toString).createCollection("myCollection", null)
    } else {
      val fongo = new Fongo("InMemoryMongo")
      collection = fongo.getDB("myDB").createCollection("myCollection", null)
    }

    init()
  }

  after {
    if (realWorld) {
      collection.getDB().dropDatabase();
    } else {
      collection.drop();
    }
  }
}