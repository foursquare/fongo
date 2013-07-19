package com.foursquare.fongo

import org.scalatest.Suite

import org.scalatest._
import com.mongodb.BasicDBObject

class FongoScalaTest extends FlatSpec with ShouldMatchers {

    "Fongo" should "not throw npe" in  {
        val fongo = new Fongo("InMemoryMongo")
        val db = fongo.getDB("myDB")
        val col = db.createCollection("myCollection", null)
        val result = col.findOne()
        assert(result == null)
    }

    "Insert" should "work" in {
        val collection = new Fongo("InMemoryMongo").getDB("myDB").createCollection("myCollection", null)

        collection.insert(new BasicDBObject("basic", "basic"))

        assert(1 === collection.count())
    }
}