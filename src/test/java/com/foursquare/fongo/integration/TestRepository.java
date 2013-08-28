package com.foursquare.fongo.integration;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * User: william
 * Date: 28/08/13
 */
public interface TestRepository extends MongoRepository<SpringFongoTest.ReferencedObject, String> {
}
