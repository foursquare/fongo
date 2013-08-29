package com.foursquare.fongo.integration;

import com.foursquare.fongo.Fongo;
import com.mongodb.Mongo;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpringRepositoryFongoTest.MongoConfig.class)
public class SpringRepositoryFongoTest {
  @Autowired
  ApplicationContext context;

  @Test
  public void test() {
    MongoOperations operations = context.getBean(MongoOperations.class);
    operations.save(new Doc("A"));
    operations.save(new Doc("B"));
    operations.save(new Doc("C"));

    assertEquals(3, operations.findAll(Doc.class).size());
    assertEquals(3, operations.find(Query.query(Criteria.where("id").in("A", "B", "C")), Doc.class).size());
    assertEquals(3, operations.find(Query.query(Criteria.where("id").in("A", "B", "C")).with(new PageRequest(0, 10)), Doc.class).size());
    assertEquals(3, operations.find(Query.query(Criteria.where("id").in("A", "B", "C")).with(new Sort(Sort.Direction.ASC, "id")), Doc.class).size());
    assertEquals(3, operations.find(Query.query(Criteria.where("id").in("A", "B", "C")).with(new PageRequest(0, 10, Sort.Direction.DESC, "id")), Doc.class).size());
  }

  @Configuration
  @EnableMongoRepositories
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
  public static class Doc {
    private String id;

    public Doc(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
  }

}
