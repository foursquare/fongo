package com.foursquare.fongo

import _root_.com.foursquare.fongo.impl.Util
import _root_.com.mongodb._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.ParallelTestExecution

// TODO : sum of double value ($sum : 1.3)
// sum of "1" (String) must return 0.

// Handle $group { _id = 0}
@RunWith(classOf[JUnitRunner])
class FongoAggregationScalaTest extends FongoAbstractTest with ParallelTestExecution {
  // If you want to test against real world (a real mongodb client).
  val realWorld = !true

  override def init = {
    collection.insert(new BasicDBObject("myId", "p0").append("date", 1))
    collection.insert(new BasicDBObject("myId", "p0").append("date", 2))
    collection.insert(new BasicDBObject("myId", "p0").append("date", 3))
    collection.insert(new BasicDBObject("myId", "p0").append("date", 4))
    collection.insert(new BasicDBObject("myId", "p0").append("date", 5))
    collection.insert(new BasicDBObject("myId", "p1").append("date", 6))
    collection.insert(new BasicDBObject("myId", "p2").append("date", 7))
    collection.insert(new BasicDBObject("myId", "p3").append("date", 0))
    collection.insert(new BasicDBObject("myId", "p0"))
    collection.insert(new BasicDBObject("myId", "p4"))
  }

  test("Fongo should handle avg of field") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val groupFields = new BasicDBObject("_id", null)
    groupFields.put("date", new BasicDBObject("$avg", "$date"))
    val group = new BasicDBObject("$group", groupFields)
    val output = collection.aggregate(`match`, group)
    var result: Number = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number])
      }
    }

    assert(result.isInstanceOf[Double], "must be a Double but was a " + result.getClass)
    assert(3.5D === result)
  }

  test("Fongo should handle avg of double field") {
    collection.insert(new BasicDBObject("myId", "p4").append("date", 1D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 2D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 3D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 4D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 5D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 6D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 7D))
    collection.insert(new BasicDBObject("myId", "p4").append("date", 10D))

    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", "p4"))
    val group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("date", new BasicDBObject("$avg", "$date")))
    val output = collection.aggregate(`match`, group)
    var result: Number = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number])
      }
    }

    assert(result.isInstanceOf[Double])
    assert(4.75D === result)
  }

  test("Fongo should handle sum of field") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val groupFields = new BasicDBObject("_id", null)
    groupFields.put("date", new BasicDBObject("$sum", "$date"))
    val group = new BasicDBObject("$group", groupFields)
    val output = collection.aggregate(`match`, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(21 === result)
  }

  // Group with "simple _id"
  test("Fongo should handle sum of number") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("sum", new BasicDBObject("$sum", 2)))
    val output = collection.aggregate(`match`, group)
    var result: Number = -1
    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
    if (resultAggregate != null && resultAggregate.containsField("sum")) {
      result = (resultAggregate.get("sum").asInstanceOf[Number])
    }

    assert(14 === result)
  }

  // Group with "simple _id"
  test("Fongo should handle sum of field grouped by myId") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1))
    val group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", "$date")))

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

  test("Fongo should handle sum of value grouped by myId") {
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))))
    val sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1))
    val group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", 2)))

    val output = collection.aggregate(`match`, group, sort)
    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val resultAggregate = (output.getCommandResult.get("result").asInstanceOf[BasicDBList])
    assert(resultAggregate.get(0).asInstanceOf[DBObject].get("_id") === "p0")
    assert(resultAggregate.get(0).asInstanceOf[DBObject].get("count") === 12)
    assert(resultAggregate.get(1).asInstanceOf[DBObject].get("_id") === "p1")
    assert(resultAggregate.get(1).asInstanceOf[DBObject].get("count") === 2)
  }

  test("Fongo should unwind list") {
    collection.insert(new BasicDBObject("author", "william").append("tags", Util.list("scala", "java", "mongo")))
    val matching = new BasicDBObject("$match", new BasicDBObject("author", "william"))
    val unwind = new BasicDBObject("$unwind", "$tags")

    val output = collection.aggregate(matching, unwind)

    // Assert
    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val result: BasicDBList = output.getCommandResult.get("result").asInstanceOf[BasicDBList]
    assert(3 === result.size())
    assert(result.get(0).asInstanceOf[DBObject].get("author") === "william")
    assert(result.get(0).asInstanceOf[DBObject].get("tags") === "scala")
    assert(result.get(1).asInstanceOf[DBObject].get("author") === "william")
    assert(result.get(1).asInstanceOf[DBObject].get("tags") === "java")
    assert(result.get(2).asInstanceOf[DBObject].get("author") === "william")
    assert(result.get(2).asInstanceOf[DBObject].get("tags") === "mongo")
  }

  test("Fongo should unwind empty list") {
    collection.insert(new BasicDBObject("author", "william").append("tags", Util.list()))
    val matching = new BasicDBObject("$match", new BasicDBObject("author", "william"))
    val project = new BasicDBObject("$project", new BasicDBObject("author", 1).append("tags", 1))
    val unwind = new BasicDBObject("$unwind", "$tags")

    val output = collection.aggregate(matching, project, unwind)

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val result: BasicDBList = output.getCommandResult.get("result").asInstanceOf[BasicDBList]
    assert(0 === result.size())
  }

}