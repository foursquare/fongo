package com.foursquare.fongo

import org.scalatest._
import com.mongodb._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.util.UUID

@RunWith(classOf[JUnitRunner])
class FongoAggregationScalaTest extends FunSuite with BeforeAndAfter {
  // If you want to test against real world (a real mongodb client).
  val realWorld = !true

  var collection: DBCollection = _

  before {
    if (realWorld) {
      val mongo = new MongoClient("localhost")
      collection = mongo.getDB(UUID.randomUUID().toString).createCollection("myCollection", null)
    } else {
      val fongo = new Fongo("InMemoryMongo")
      collection = fongo.getDB("myDB").createCollection("myCollection", null)
    }

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

  after {
    if (realWorld) {
      collection.getDB().dropDatabase();
    } else {
      collection.drop();
    }
  }

  test("Fongo should handle min") {

    val list: BasicDBList = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match`: DBObject = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$min", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(1 === result)
  }

  test("Fongo should handle max") {
    val list: BasicDBList = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match`: DBObject = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$max", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(6 === result)
  }

  test("Fongo should handle last") {
    val list = new BasicDBList
    list.add("p3")
    list.add("p2")
    list.add("p1")
    val `match`: DBObject = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$last", "$date"))
    val sort = new BasicDBObject("$sort", new BasicDBObject("date", 1))
    val group = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, sort, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(7 === result)
  }

  test("Fongo should handle last null value") {
    val list = new BasicDBList
    list.add("p4")
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$last", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, group)
    var result = false
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        assert(null == resultAggregate.get("date"))
        result = true
      }
    }

    assert(true === result, "Result not found")
  }

  test("Fongo should handle first") {
    val list = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$first", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(1 === result)
  }

  test("Fongo should handle first null") {
    val list = new BasicDBList
    list.add("p4")
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$first", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, group)
    var result = false
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        assert(null == resultAggregate.get("date"))
        result = true
      }
    }

    assert(true === result, "Result not found")
  }

  test("Fongo should handle max with limit") {
    val list: BasicDBList = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match`: DBObject = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val limit = new BasicDBObject("$limit", 3)
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$max", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, limit, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(3 === result)
  }

  test("Fongo should handle min with skip") {
    val list: BasicDBList = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match`: DBObject = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val skip = new BasicDBObject("$skip", 3)
    val groupFields: DBObject = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$min", "$date"))
    val group: DBObject = new BasicDBObject("$group", groupFields)
    val output: AggregationOutput = collection.aggregate(`match`, skip, group)
    var result = 0
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(4 === result)
  }


  test("Fongo should handle sum of field") {
    val list = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields = new BasicDBObject("_id", "0")
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

  test("Fongo should handle sum of number") {
    val list = new BasicDBList
    list.add("p0")
    list.add("p1")
    val `match` = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", list)))
    val groupFields = new BasicDBObject("_id", "0")
    groupFields.put("date", new BasicDBObject("$sum", "2"))
    val group = new BasicDBObject("$group", groupFields)
    val output = collection.aggregate(`match`, group)
    var result = 0
    println(output)
    if (output.getCommandResult.ok && output.getCommandResult.containsField("result")) {
      val resultAggregate: DBObject = (output.getCommandResult.get("result").asInstanceOf[DBObject]).get("0").asInstanceOf[DBObject]
      if (resultAggregate != null && resultAggregate.containsField("date")) {
        result = (resultAggregate.get("date").asInstanceOf[Number]).intValue
      }
    }

    assert(14 === result)
  }
}