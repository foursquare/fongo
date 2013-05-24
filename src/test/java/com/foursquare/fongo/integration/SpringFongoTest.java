package com.foursquare.fongo.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.foursquare.fongo.Fongo;
import com.mongodb.Mongo;
import java.io.Serializable;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.hateoas.Identifiable;

public class SpringFongoTest {

	@Test
	public void dBRefFindWorks() {
		ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
		MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

		MainObject mainObject = new MainObject();

		ReferencedObject referencedObject = new ReferencedObject();

		mainObject.setReferencedObject(referencedObject);

		mongoOperations.save(referencedObject);
		mongoOperations.save(mainObject);

		MainObject foundObject = mongoOperations.findOne(
				new Query(Criteria.where("referencedObject.$id").is(ObjectId.massageToObjectId(referencedObject.getId()))),
				MainObject.class);

		assertNotNull("should have found an object", foundObject);
		assertEquals("should find a ref to an object", referencedObject.getId(), foundObject.getId());

	}

	@Configuration
	public static class MongoConfig extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "db";
		}

		@Override
		@Bean
		public Mongo mongo() throws Exception {
			return new Fongo("spring-test").getMongo();
		}
	}

	@Document
	public static class ReferencedObject implements Serializable, Identifiable<String> {

		private static final long serialVersionUID = 1L;
		@Id
		private String id;

		@Override
		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

	}

	@Document
	public static class MainObject implements Serializable, Identifiable<String> {

		private static final long serialVersionUID = 1L;

		@Id
		private String id;

		@DBRef
		private ReferencedObject referencedObject;

		@Override
		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public ReferencedObject getReferencedObject() {
			return referencedObject;
		}

		public void setReferencedObject(ReferencedObject referencedObject) {
			this.referencedObject = referencedObject;
		}

	}
}
