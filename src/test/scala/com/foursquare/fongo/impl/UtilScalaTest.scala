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

}