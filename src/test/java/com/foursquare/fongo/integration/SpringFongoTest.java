package com.foursquare.fongo.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.foursquare.fongo.Fongo;
import com.mongodb.Mongo;
import java.io.Serializable;
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
    
    TestDoc tdoc = new TestDoc();
    tdoc.setIdTest("1");
    TestSuite ts = new TestSuite();
    ts.setIdTestSuite("2");
    tdoc.setTestSuite(ts);
    mongoOperations.save(ts);
    mongoOperations.save(tdoc);
    
    TestDoc foundDoc = mongoOperations.findOne(new Query(Criteria.where("_id").is("1")), TestDoc.class);
    assertNotNull("should have found a doc", foundDoc);
    assertEquals("should find a ref to a testsuite", "2", foundDoc.getTestSuite().getId());
    
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
  public static class TestSuite implements Serializable, Identifiable<String> {

    @Id
    private String idTestSuite;
    
    @Override
    public String getId() {
      return idTestSuite;
    }

    public String getIdTestSuite() {
      return idTestSuite;
    }

    public void setIdTestSuite(String idTestSuite) {
      this.idTestSuite = idTestSuite;
    }
  }

  @Document
  public static class TestDoc implements Serializable, Identifiable<String> {

    public static final String TEST_SUITE = "test_suite";

    @Id
    private String idTest;

    @DBRef
    private TestSuite testSuite;

    @Override
    public String getId() {
      return this.idTest;
    }

    public String getIdTest() {
      return idTest;
    }

    public void setIdTest(String idTest) {
      this.idTest = idTest;
    }

    public void setTestSuite(TestSuite testSuite) {
      this.testSuite = testSuite;
    }

    public TestSuite getTestSuite() {
      return testSuite;
    }
  }
}
