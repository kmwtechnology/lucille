package com.kmwllc.lucille.connector.jdbc;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.connector.jdbc.DatabaseConnector;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DatabaseConnectorTest {

  private static final Logger log = LoggerFactory.getLogger(DatabaseConnectorTest.class);
  
  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
      "", "db-test-start.sql", "db-test-end.sql");

  private Publisher publisher;
  private PersistingLocalMessageManager manager;
  
  private String testRunId = "testRunId";
  
  @Before
  public void initTestMode() {
    // set lucile into loopback mode for local / standalone testing.
    manager = new PersistingLocalMessageManager();
    publisher = new PublisherImpl(manager);
  }

  @Test
  public void testDatabaseConnector() throws Exception {
    
    // Create the test config
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name,type from animal order by id");
    configValues.put("idField", "id");
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    
    // start the connector 
    connector.start(publisher);

    // Confirm there were 3 results.
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docsSentForProcessing.size());
    
    // System.out.println(docsSentForProcessing.get(0));
    // confirm first doc is 1
    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals("Matt", docsSentForProcessing.get(0).getStringList("name").get(0));
    assertEquals("Human", docsSentForProcessing.get(0).getStringList("type").get(0));


    assertEquals("2", docsSentForProcessing.get(1).getId());
    assertEquals("Sonny", docsSentForProcessing.get(1).getStringList("name").get(0));
    assertEquals("Cat", docsSentForProcessing.get(1).getStringList("type").get(0));

    assertEquals("3", docsSentForProcessing.get(2).getId());
    assertEquals("Blaze", docsSentForProcessing.get(2).getStringList("name").get(0));
    assertEquals("Cat", docsSentForProcessing.get(2).getStringList("type").get(0));
    
  }
  
  // TODO: work through the rest of these examples and validate them
  // @Test
  public void testJoiningDatabaseConnector() throws Exception {
    
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from animal");
    
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<String>();
    otherSql.add("select * from meal order by animal_id");
    ArrayList<String> otherJoinFields = new ArrayList<String>();
    otherJoinFields.add("animal_id");
    configValues.put("otherSql", otherSql);
    configValues.put("idField", "id");
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    String runId = UUID.randomUUID().toString();
    // create a publisher to record all the docs sent to it.  
    // run the connector
    connector.start(publisher);
    
    
    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());
    
    
    for (Document d : docs ) {
      System.err.println(d.toString());
    }
    // Ok.. need to validate. 
    
    //    for (Document doc : publisher.getPublishedDocs()) {
    //      System.out.println(doc);
    //    }
    //    // TODO?
    //    assertEquals(3, publisher.getPublishedDocs().size());
    //    // TODO: more validations.

  }
  
  
  // @Test
  public void testCollapsingDatabaseConnector() throws Exception {
    // TODO: implement me
    
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from animal , meal where animal.id = meal.animal_id order by animal.id asc");
    configValues.put("idField", "id");
    configValues.put("collapse", true);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    String runId = UUID.randomUUID().toString();
    // create a publisher to record all the docs sent to it.  
    // run the connector
    connector.start(publisher);
    // TODO: 
    //    for (Document doc : publisher.getPublishedDocs()) {
    //      System.out.println(doc);
    //    }
    //    // TODO?
    //    assertEquals(3, publisher.getPublishedDocs().size());
    // TODO: more validations.

  }

  
  

}
