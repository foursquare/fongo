package com.mongodb;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.foursquare.fongo.Fongo;

public class FongoDBCollectionTest {
	private FongoDBCollection collection;
	
	@Before
	public void setUp() {
		collection = (FongoDBCollection) new Fongo("test").getDB("test").getCollection("test");
	}
	
	@Test
	public void applyProjectionsInclusionsOnly() {
		BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
		DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 1));
		DBObject expected = new BasicDBObject().append("_id", "_id").append("b", "b");
		
		assertEquals("applied", expected, actual);
	}
	
	@Test
	public void applyProjectionsInclusionsWithoutId() {
		BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
		DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 1));
		BasicDBObject expected = new BasicDBObject().append("b", "b");
		
		assertEquals("applied", expected, actual);
	}
	
	@Test
	public void applyProjectionsExclusionsOnly() {
		BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
		DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 0));
		BasicDBObject expected = new BasicDBObject().append("_id", "_id").append("a", "a");
		
		assertEquals("applied", expected, actual);
	}
	
	@Test
	public void applyProjectionsExclusionsWithoutId() {
		BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
		DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 0));
		BasicDBObject expected = new BasicDBObject().append("a", "a");
		
		assertEquals("applied", expected, actual);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void applyProjectionsInclusionsAndExclusionsMixedThrowsException() {
		BasicDBObject obj = new BasicDBObject().append("_id", "_id").append("a", "a").append("b", "b");
		collection.applyProjections(obj, new BasicDBObject().append("a", 1).append("b", 0));
	}
}
