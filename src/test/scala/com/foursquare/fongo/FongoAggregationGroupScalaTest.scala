package com.foursquare.fongo

import _root_.com.foursquare.fongo.impl.Util
import _root_.com.mongodb._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// TODO : sum of double value ($sum : 1.3)
// sum of "1" (String) must return 0.

// Handle $group { _id = 0}
@RunWith(classOf[JUnitRunner])
class FongoAggregationGroupScalaTest extends FongoAbstractTest {
  // If you want to test against real world (a real mongodb client).
  val realWorld = true

  override def init() = {
  }

  // Group with "simple _id"
  test("Fongo should handle sum of field grouped by myId with complex id.") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val groupFields = new BasicDBObject("_id", "$myId")
    groupFields.put("count", new BasicDBObject("$sum", "$date"))
    val group = new BasicDBObject("$group", groupFields)
    val sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1))

    val output = collection.aggregate(`match`, group, sort)
    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val resultAggregate = (output.getCommandResult.get("result").asInstanceOf[BasicDBList])
    assert(resultAggregate.size == 2)
    assert(resultAggregate.get(0).asInstanceOf[DBObject].get("_id") === "p0")
    assert(resultAggregate.get(0).asInstanceOf[DBObject].get("count") === 15)
    assert(resultAggregate.get(1).asInstanceOf[DBObject].get("_id") === "p1")
    assert(resultAggregate.get(1).asInstanceOf[DBObject].get("count") === 6)
  }

}