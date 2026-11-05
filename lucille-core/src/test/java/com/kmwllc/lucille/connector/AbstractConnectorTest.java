package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class AbstractConnectorTest {

  private class MyConnector extends AbstractConnector {

    public static final Spec SPEC = SpecBuilder.connector().build();

    public MyConnector(Config config) {
      super(config);
    }

    @Override
    public void execute(Publisher publisher) throws ConnectorException {

    }
  }

  @Test
  public void testCreateDocId() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put("name", "connector1");
    map.put("pipeline", "pipeline1");
    map.put("docIdPrefix", "myprefix_");
    Config config = ConfigFactory.parseMap(map);
    MyConnector connector = new MyConnector(config);
    assertEquals("myprefix_", connector.getDocIdPrefix());
    assertEquals("myprefix_id1", connector.createDocId("id1"));
  }

  @Test
  public void testCreateDocIdRejectsNullAndBlank() throws Exception {
    // a null source id would otherwise become the text "null", shared by every such record
    Map<String, String> map = new HashMap<>();
    map.put("name", "connector1");
    map.put("pipeline", "pipeline1");
    map.put("docIdPrefix", "myprefix_");
    MyConnector connector = new MyConnector(ConfigFactory.parseMap(map));

    assertThrows(IllegalArgumentException.class, () -> connector.createDocId(null));
    assertThrows(IllegalArgumentException.class, () -> connector.createDocId(""));
    assertThrows(IllegalArgumentException.class, () -> connector.createDocId("   "));
    // a usable id is used exactly as given, without trimming
    assertEquals("myprefix_ id1 ", connector.createDocId(" id1 "));
  }

  @Test
  public void testDefaultNames() throws Exception {
    List<Connector> connectors = Connector.fromConfig(ConfigFactory.load("AbstractConnectorTest/defaultNames.conf"));
    assertEquals(3, connectors.size());
    assertEquals("connector_1", connectors.get(0).getName());
    assertEquals("connector_2", connectors.get(1).getName());
    assertEquals("myConnector", connectors.get(2).getName());
  }

  @Test(expected = ConnectorException.class)
  public void testDuplicateNames() throws Exception {
    Connector.fromConfig(ConfigFactory.load("AbstractConnectorTest/duplicateNames.conf"));
  }

  // names that differ only by case are duplicates, since they would map to the same state table
  @Test
  public void testDuplicateNamesIgnoreCase() {
    Config config = ConfigFactory.parseString("connectors: ["
        + "{class: \"com.kmwllc.lucille.connector.FileConnector\", paths: [\"src/test/resources/RunnerTest/singleDoc.csv\"], "
        + "fileHandlers: {csv: {}}, name: \"file\", pipeline: \"pipeline1\"},"
        + "{class: \"com.kmwllc.lucille.connector.FileConnector\", paths: [\"src/test/resources/RunnerTest/singleDoc.csv\"], "
        + "fileHandlers: {csv: {}}, name: \"File\", pipeline: \"pipeline2\"}]");
    ConnectorException e = assertThrows(ConnectorException.class, () -> Connector.fromConfig(config));
    assertTrue(e.getMessage().contains("share the same name"));
  }

  // a FileConnector without its required paths, and without a name
  private static final String UNNAMED_INVALID_CONNECTOR =
      "{class: \"com.kmwllc.lucille.connector.FileConnector\", fileHandlers: {csv: {idField: \"id\"}}}";

  @Test
  public void testValidationUsesTheRunName() {
    Config config = ConfigFactory.parseString(UNNAMED_INVALID_CONNECTOR);
    List<Exception> exceptions = Connector.getConnectorConfigExceptions(config, 2);
    assertEquals(1, exceptions.size());
    String message = exceptions.get(0).getMessage();
    assertTrue(message, message.contains("connector_2"));
    assertFalse(message, message.contains("noname"));
  }

  @Test
  public void testFromConfigPassesInvalidConfigThrough() {
    Config config = ConfigFactory.parseString("connectors: [" + UNNAMED_INVALID_CONNECTOR + "]");
    // the constructor's exception, not an InvocationTargetException wrapping it
    assertThrows(IllegalArgumentException.class, () -> Connector.fromConfig(config));
  }

  @Test
  public void testFromConfigUnknownClass() {
    Config config = ConfigFactory.parseString("connectors: [{class: \"com.example.NoSuchConnector\"}]");
    ConnectorException e = assertThrows(ConnectorException.class, () -> Connector.fromConfig(config));
    assertTrue(e.getCause() instanceof ClassNotFoundException);
  }

  @Test
  public void testFromConfigClassThatIsNotAConnector() {
    Config config = ConfigFactory.parseString("connectors: [{class: \"java.lang.String\"}]");
    assertThrows(IllegalArgumentException.class, () -> Connector.fromConfig(config));
  }

}
