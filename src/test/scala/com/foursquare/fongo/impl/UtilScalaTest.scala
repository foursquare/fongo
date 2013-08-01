package com.foursquare.fongo.impl

import org.scalatest._
import com.mongodb._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UtilScalaTest extends FunSuite {

  test("extractField must handle simple case") {
    val obj = new BasicDBObject("field", "value")

    assert("value" === Util.extractField(obj, "field"))
  }

  test("extractField must handle one step case") {
    val obj = new BasicDBObject("field", new BasicDBObject("field2", "value"))

    assert("value" === Util.extractField(obj, "field.field2"))
  }

  test("extractField must handle tree step case") {
    val obj = new BasicDBObject("field", new BasicDBObject("field1", "badvalue").append("field2", new BasicDBObject("field1", "value")))

    assert("value" === Util.extractField(obj, "field.field2.field1"))
  }

  test("contains Field must return true for simple case") {
    val obj = new BasicDBObject("field", "value")

    assert(true === Util.containsField(obj, "field"))
  }

  test("containsField must handle one step case") {
    val obj = new BasicDBObject("field", new BasicDBObject("field2", "value"))

    assert(true === Util.containsField(obj, "field.field2"))
  }

  test("containsField must handle tree step case") {
    val obj = new BasicDBObject("field", new BasicDBObject("field1", "badvalue").append("field2", new BasicDBObject("field1", "value")))

    assert(true === Util.containsField(obj, "field.field2.field1"))
  }

  test("containsField must return false when not present") {
    val obj = new BasicDBObject("field", new BasicDBObject("field1", "badvalue").append("field2", new BasicDBObject("field1", "value").append("nullfield", null)))

    assert(false === Util.containsField(obj, "field2"))
    assert(false === Util.containsField(obj, "field2.field2"))
    assert(false === Util.containsField(obj, "field.field3"))
    assert(false === Util.containsField(obj, "field.field3"))
  }

  test("containsField must return true when present but null") {
    val obj = new BasicDBObject("field", new BasicDBObject("field1", "badvalue").append("field2", new BasicDBObject("field1", "value").append("nullfield", null)))

    assert(true === Util.containsField(obj, "field.field2.nullfield"))
  }
}