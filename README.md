# fongo

Fongo is an in-memory java implementation of mongo.  It intercepts calls to the standard mongo-java-driver for 
finds, updates, inserts, removes and other methods.  The primary use is for lightweight unit testing where you
don't want to spin up a mongo process.

The original hope was that this would be significantly faster than using a real mongo server, however, it's still not clear if that's the case.  Another goal was to make it easier to parallelize tests, but that could also be achieved
by fixing each thread to a uniquely named database.

## Usage

1. Clone this repo and build the jar:
    
    mvn package

1. Copy jar to classpath

1. Use in place of regular `com.mongodb.Mongo` instance:


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

## Scope

fongo doesn't implement all mongo functionality. most query and update syntax is supported.  MapReduce,
gridfs, and capped collections are not supported.  Also, there is no index support other than the \_id index.
Fongo uses a LinkedHashMap internally with the \_id as the key.

## Implementation Details

Fongo depends on mockito to hijack the com.mongodb.Mongo class.  It has a "provided" dependency on the mongo-java-driver and was tested with 2.7.2.

Fongo should be thread safe. All read and write operations on collections are synchronized.  It's pretty course, but
should be good enough for simple testing.  Fongo doesn't have any shared state (no statics).  Each Fongo instance is completely independent.

## Usage Details

    //create an instance with lots of debug logging (uses printlns :-/ )
    new Fongo("mongo server 1", true)

    // Fongo instance methods
    
    // get all created databases (they are created automatically the first time requested)
    Collection<DB> dbs = fongo.getUsedDatabases();
    // also
    List<String> dbNames = fongo.getDatabaseNames();
    // also
    fongo.dropDatabase("dbName");

    // get an instance of the hijacked com.mongodb.Mongo
    Mongo mongo = fongo.getMongo();

## Todo

* more testing
* publish to one of the maven repos
* find an actual use for this project
