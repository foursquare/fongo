package com.foursquare.fongo;

import java.util.Collection;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.MongoException;

public interface MongoConnection {
  /**
   * gets a database object
   * @param dbname the database name
   * @return
   */
  public DB getDB( String dbname );

  /**
   * gets a collection of DBs used by the driver since this Mongo instance was created.
   * This may include DBs that exist in the client but not yet on the server.
   * @return
   */
  public Collection<DB> getUsedDatabases();
  /**
   * gets a list of all database names present on the server
   * @return
   * @throws MongoException
   */
  public List<String> getDatabaseNames() throws MongoException;


  /**
   * Drops the database if it exists.
   * @param dbName name of database to drop
   * @throws MongoException
   */
  public void dropDatabase(String dbName) throws MongoException;
}
