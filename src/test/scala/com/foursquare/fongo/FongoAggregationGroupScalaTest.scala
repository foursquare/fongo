package com.foursquare.fongo

import _root_.com.foursquare.fongo.impl.Util
import _root_.com.mongodb._
import _root_.com.mongodb.util.JSON
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._
import org.scalatest.ParallelTestExecution

@RunWith(classOf[JUnitRunner])
class FongoAggregationGroupScalaTest extends FongoAbstractTest with ParallelTestExecution {
  // If you want to test against real world (a real mongodb client).
  val realWorld = !true

  lazy val zips = {
    import scala.io.Source
    val source = Source.fromInputStream(this.getClass.getResourceAsStream("/zips.json"))
    source.getLines().map(JSON.parse(_).asInstanceOf[DBObject])
  }

  override def init() = {
  }

  // see http://stackoverflow.com/questions/11418985/mongodb-aggregation-framework-group-over-multiple-values
  test("Fongo should handle 'States with Populations Over 5 Million'") {
    zips.foreach(collection.insert(_))
    val pipeline = JSON.parse(
      """
        |[{ $group :
        |                         { _id : "$state",
        |                           totalPop : { $sum : "$pop" } } },
        |{ $match : {totalPop : { $gte : 5000000 } } },
        |{ $sort : {_id:1}}
        |]
      """.stripMargin).asInstanceOf[java.util.List[DBObject]]

    val output = collection.aggregate(pipeline(0), pipeline(1), pipeline(2))
    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val resultAggregate = (output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]])
    assert(resultAggregate.size === 3)
    assert(resultAggregate.get(0).get("_id") === "MA")
    assert(resultAggregate.get(0).get("totalPop") === 6016425)
    assert(resultAggregate.get(1).get("_id") === "NJ")
    assert(resultAggregate.get(1).get("totalPop") === 7730188)
    assert(resultAggregate.get(2).get("_id") === "NY")
    assert(resultAggregate.get(2).get("totalPop") === 12950936)
  }

  test("Fongo should handle 'Largest and Smallest Cities by State'") {
    zips.foreach(collection.insert(_))
    val pipeline = JSON.parse(
      """
        |[   { $group:
        |      { _id: { state: "$state", city: "$city" },
        |        pop: { $sum: "$pop" } } },
        |    { $sort: { pop: 1 } },
        |    { $group:
        |      { _id : "$_id.state",
        |        biggestCity:  { $last: "$_id.city" },
        |        biggestPop:   { $last: "$pop" },
        |        smallestCity: { $first: "$_id.city" },
        |        smallestPop:  { $first: "$pop" } } },
        |    { $project:
        |      { _id: 0,
        |        state: "$_id",
        |        biggestCity:  { name: "$biggestCity",  pop: "$biggestPop" },
        |        smallestCity: { name: "$smallestCity", pop: "$smallestPop" } } },
        |    { $sort : { "biggestCity.name" : 1 } }
        |
        |]      """.stripMargin).asInstanceOf[java.util.List[DBObject]]
    val output = collection.aggregate(pipeline(0), pipeline(1), pipeline(2), pipeline(3), pipeline(4))

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))

    val resultAggregate = (output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]])
    assert(resultAggregate.size === 8)
    assert("BRIDGEPORT" === Util.extractField(resultAggregate(0), "biggestCity.name"))
    assert(141638 === Util.extractField(resultAggregate(0), "biggestCity.pop"))
    assert("EAST KILLINGLY" === Util.extractField(resultAggregate(0), "smallestCity.name"))
    assert(25 === Util.extractField(resultAggregate(0), "smallestCity.pop"))
    assert("WORCESTER" === Util.extractField(resultAggregate.last, "biggestCity.name"))
    assert(169856 === Util.extractField(resultAggregate.last, "biggestCity.pop"))
    assert("BUCKLAND" === Util.extractField(resultAggregate.last, "smallestCity.name"))
    assert(16 === Util.extractField(resultAggregate.last, "smallestCity.pop"))
  }

  // see http://stackoverflow.com/questions/8161444/mongodb-getting-list-of-values-by-using-group
  test("Fongo must handle $push in group") {
    val objects = JSON.parse(
      """
        |[{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 2,
        | "name": "n2"
        |},
        |{
        | a_id: 1,
        | "name": "n3"
        |},
        |{
        | a_id: 1,
        | "name": "n4"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |}]
      """.stripMargin).asInstanceOf[java.util.List[DBObject]]
    objects.toList.foreach(collection.insert(_))

    val group = JSON.parse("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}").asInstanceOf[DBObject]

    // Aggregate
    val output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)))

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))
    val result = output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]]

    assert(result.size() === 2)
    assert(1 === Util.extractField(result.get(0), "_id"))
    assert(Util.list("n1", "n3", "n4") === Util.extractField(result.get(0), "name"))
    assert(2 === Util.extractField(result.get(1), "_id"))
    assert(Util.list("n2", "n5") === Util.extractField(result.get(1), "name"))
  }

  test("Fongo must handle $push in group with same value") {
    val objects = JSON.parse(
      """
        |[{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |},
        |{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 1,
        | "name": "n2"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |}]
      """.stripMargin).asInstanceOf[java.util.List[DBObject]]
    objects.toList.foreach(collection.insert(_))

    val group = JSON.parse("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}").asInstanceOf[DBObject]

    // Aggregate
    val output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)))

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))
    val result = output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]]

    assert(result.size() === 2)
    assert(1 === Util.extractField(result.get(0), "_id"))
    assert(Util.list("n1", "n1", "n2") === Util.extractField(result.get(0), "name"))
    assert(2 === Util.extractField(result.get(1), "_id"))
    assert(Util.list("n5", "n5") === Util.extractField(result.get(1), "name"))
  }

  test("Fongo must handle $addToSet in group") {
    val objects = JSON.parse(
      """
        |[{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 2,
        | "name": "n2"
        |},
        |{
        | a_id: 1,
        | "name": "n3"
        |},
        |{
        | a_id: 1,
        | "name": "n4"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |}]
      """.stripMargin).asInstanceOf[java.util.List[DBObject]]
    objects.toList.foreach(collection.insert(_))

    val group = JSON.parse("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}").asInstanceOf[DBObject]

    // Aggregate
    val output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)))

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))
    val result = output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]]

    assert(result.size() === 2)
    assert(1 === Util.extractField(result.get(0), "_id"))
    assert(Util.list("n1", "n3", "n4") === Util.extractField(result.get(0), "name"))
    assert(2 === Util.extractField(result.get(1), "_id"))
    assert(Util.list("n2", "n5") === Util.extractField(result.get(1), "name"))
  }

  test("Fongo must handle $addToSet in group with same value") {
    val objects = JSON.parse(
      """
        |[{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |},
        |{
        | a_id: 1,
        | "name": "n1"
        |},
        |{
        | a_id: 1,
        | "name": "n2"
        |},
        |{
        | a_id: 2,
        | "name": "n5"
        |}]
      """.stripMargin).asInstanceOf[java.util.List[DBObject]]
    objects.toList.foreach(collection.insert(_))

    val group = JSON.parse("{$group: { '_id': '$a_id', 'name': { $addToSet: '$name'}}}").asInstanceOf[DBObject]

    // Aggregate
    val output = collection.aggregate(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1)))

    assert(output.getCommandResult.ok)
    assert(output.getCommandResult.containsField("result"))
    val result = output.getCommandResult.get("result").asInstanceOf[java.util.List[DBObject]]

    assert(result.size() === 2)
    assert(1 === Util.extractField(result.get(0), "_id"))
    assert(Util.extractField(result.get(0), "name").asInstanceOf[java.util.List[String]].contains("n1"))
    assert(Util.extractField(result.get(0), "name").asInstanceOf[java.util.List[String]].contains("n2"))
    assert(Util.extractField(result.get(0), "name").asInstanceOf[java.util.List[String]].size === 2)
    assert(2 === Util.extractField(result.get(1), "_id"))
    assert(Util.list("n5") === Util.extractField(result.get(1), "name"))
  }

}