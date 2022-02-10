package com.kmwllc.lucille.connector.jdbc;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.kmwllc.lucille.core.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.message.PersistingLocalMessageManager;
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
  private String connectorName = "testConnector";
  private String pipelineName = "testPipeline";
  
  @Before
  public void initTestMode() throws Exception {
    // set lucille into loopback mode for local / standalone testing.
    manager = new PersistingLocalMessageManager();
    publisher = new PublisherImpl(ConfigFactory.empty(), manager, testRunId, pipelineName);
  }
  
  @Test
  public void testDatabaseConnector() throws Exception {
    
    // Create the test config
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
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
    connector.execute(publisher);

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

  @Test
  public void testCompaniesQuery() throws ConnectorException {
    HashMap<String,Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select company_id, name from companies order by company_id");
    configValues.put("idField", "company_id");
    configValues.put("docIdPrefix", "company-");

    Config config = ConfigFactory.parseMap(configValues);

    DatabaseConnector connector = new DatabaseConnector(config);

    connector.execute(publisher);

    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(2, docsSentForProcessing.size());

    // The doc ID should have the 'company-' prefix
    assertEquals("company-1-1", docsSentForProcessing.get(0).getId());
    // There should also be a company_id field containing the company ID
    assertEquals("1-1", docsSentForProcessing.get(0).getStringList("company_id").get(0));
    assertEquals("Acme", docsSentForProcessing.get(0).getStringList("name").get(0));

    assertEquals("company-1-2", docsSentForProcessing.get(1).getId());
    assertEquals("1-2", docsSentForProcessing.get(1).getStringList("company_id").get(0));
    // The name field shouldn't be set because the value was null in the database
    assertFalse(docsSentForProcessing.get(1).has("name"));
  }

  @Test
  public void testJoiningDatabaseConnector() throws Exception {
    
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);

    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name from animal");
    configValues.put("idField", "id");
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<String>();
    otherSql.add("select id as meal_id, animal_id,name from meal order by animal_id");
    // The join fields. id goes to animal_id
    ArrayList<String> otherJoinFields = new ArrayList<String>();
    otherJoinFields.add("animal_id");
    configValues.put("otherSQLs", otherSql);
    configValues.put("otherJoinFields",otherJoinFields); 
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // run the connector
    connector.execute(publisher);
    
    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());

    // TODO: better verification / edge cases.. also formalize the "children" docs.
    String expected ="{\"id\":\"1\",\"name\":\"Matt\",\".children\":[{\"id\":\"0\",\"meal_id\":\"1\",\"animal_id\":\"1\",\"name\":\"breakfast\"},{\"id\":\"1\",\"meal_id\":\"2\",\"animal_id\":\"1\",\"name\":\"lunch\"},{\"id\":\"2\",\"meal_id\":\"3\",\"animal_id\":\"1\",\"name\":\"dinner\"}],\"run_id\":\"testRunId\"}";
    assertEquals(expected, docs.get(0).toString());

  }
  
  // TODO: not implemented yet.
  // @Test
  public void testCollapsingDatabaseConnector() throws Exception {
    // TODO: implement me
    
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select animal_id,id,name from meal order by animal_id asc");
    configValues.put("idField", "animal_id");
    configValues.put("collapse", true);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // create a publisher to record all the docs sent to it.  
    // run the connector
    
    connector.execute(publisher);
    
    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());

    for (Document d: docs) {
      System.err.println(d);
    }
    
    // TODO: 
    //    for (Document doc : publisher.getPublishedDocs()) {
    //      System.out.println(doc);
    //    }
    //    // TODO?
    //    assertEquals(3, publisher.getPublishedDocs().size());
    // TODO: more validations.

  }

  @Test
  public void testClose() throws ConnectorException {
    // Create a test config
    HashMap<String,Object> configValues = new HashMap<String,Object>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name,type from animal order by id");
    configValues.put("idField", "id");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = mock(DatabaseConnector.class);

    // call the execute method
    connector.execute(publisher);

    // check to see if close has been called (line 226 doesn't work at the moment)
    // verify(connector, times(1)).close();
  }
}
