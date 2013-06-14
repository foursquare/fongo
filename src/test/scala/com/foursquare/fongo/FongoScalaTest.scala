package com.foursquare.fongo

import org.scalatest.Suite

import org.scalatest._

class FongoScalaTest extends Suite {

  def testNpe() {
    val fongo = new Fongo("InMemoryMongo")
    val db = fongo.getDB("myDB")
    val col = db.createCollection("myCollection", null)
    val result = col.findOne()
    assert(result == null)
  }
}