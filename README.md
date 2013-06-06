# fongo

Fongo is an in-memory java implementation of mongo. It intercepts calls to the standard mongo-java-driver for 
finds, updates, inserts, removes and other methods. The primary use is for lightweight unit testing where you
don't want to spin up a mongo process.


## Usage
Add dependency to your project:

```
<dependency>
  <groupId>com.foursquare</groupId>
  <artifactId>fongo</artifactId>
  <version>1.0.9</version>
</dependency>
```

*Alternatively: clone this repo and build the jar: `mvn package` then copy jar to your classpath*

Use in place of regular `com.mongodb.Mongo` instance:

```java
import com.foursquare.fongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mognodb.DBCollection;
...
Fongo fongo = new Fongo("mongo server 1");

// once you have a DB instance, you can interact with it
// just like you would with a real one.
DB db = fongo.getDB("mydb");
DBCollection collection = db.getCollection("mycollection");
collection.insert(new BasicDBObject("name", "jon"));
```

## Scope

fongo doesn't implement all mongo functionality. most query and update syntax is supported.  MapReduce,
gridfs, and capped collections are not supported.  Also, there is no index support other than the \_id index.
Fongo uses a LinkedHashMap internally with the \_id as the key.

## Implementation Details

Fongo depends on mockito to hijack the com.mongodb.Mongo class.  It has a "provided" dependency on the mongo-java-driver and was tested with 2.10.1.
It also has a "provided" dependency on sl4j-api for logging.  If you don't already have sl4j in your project, you can add a maven dependency to the logback implementation like this:

```
<dependency> 
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.0.4</version>
</dependency>
```

Fongo should be thread safe. All read and write operations on collections are synchronized.  It's pretty course, but
should be good enough for simple testing.  Fongo doesn't have any shared state (no statics).  Each Fongo instance is completely independent.

## Usage Details

```java
// Fongo instance methods

// get all created databases (they are created automatically the first time requested)
Collection<DB> dbs = fongo.getUsedDatabases();
// also
List<String> dbNames = fongo.getDatabaseNames();
// also
fongo.dropDatabase("dbName");

// get an instance of the hijacked com.mongodb.Mongo
Mongo mongo = fongo.getMongo();
```
If you use Spring, you can configure fongo in your XML configuration context:

```xml
<bean name="fongo" class="com.foursquare.fongo.Fongo">
    <constructor-arg value="InMemoryMongo" />
</bean>
<bean id="mongo" factory-bean="fongo" factory-method="getMongo" />

<mongo:db-factory id="mongoDbFactory" mongo-ref="mongo" />

<!-- localhost settings for mongo -->
<!--<mongo:db-factory id="mongoDbFactory" />-->

<bean id="mongoTemplate" class="org.springframework.data.mongodb.core.MongoTemplate">
    <constructor-arg ref="mongoDbFactory"/>
</bean>
```

## Todo

* more testing
* the DBObject comparison code is quite right, so sorting is slightly incorrect

## Reporting Bugs and submitting patches

If you discover a bug with fongo you can file an issue in github. At the very least, please include a complete description of the problem with steps to reproduce.
If there were exceptions thrown, please include the complete stack traces. If it's not easy to explain the problem, failing test code would be helpful.
You can fork the project and add a new failing test case. 

It's even better if you can both add the test case and fix the bug. I will merge pull requests with test cases and add 
your name to the patch contributers below. Please maintain the same code formatting and style as the rest of the project.

## Author
* [Jon Hoffman](https://github.com/hoffrocket)

## Patch Contributers
* [Guido García](https://github.com/palmerabollo)
* [rid9](https://github.com/rid9)
* [aakhmerov](https://github.com/aakhmerov)
* [Eduardo Franceschi](https://github.com/efranceschi)
* [Tobias Clemson](https://github.com/tobyclemson)
* [Philipp Knobel](https://github.com/philnate)
* [Roger Lindsjö](https://github.com/rlindsjo)
* [Vadim Platono](https://github.com/dm3)
* [grahamar](https://github.com/grahamar)
* [Boris Granveaud](https://github.com/bgranvea)
