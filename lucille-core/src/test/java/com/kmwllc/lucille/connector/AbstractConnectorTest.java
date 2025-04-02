package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AbstractConnectorTest {

  private class MyConnector extends AbstractConnector {

    public MyConnector(Config config) {
      super(config, Spec.connector());
    }

    @Override
    public void execute(Publisher publisher) throws ConnectorException {

    }
  }

  @Test
  public void testCreateDocId() throws Exception {
    Map<String, String> map = new HashMap();
    map.put("name", "connector1");
    map.put("pipeline", "pipeline1");
    map.put("docIdPrefix", "myprefix_");
    Config config = ConfigFactory.parseMap(map);
    MyConnector connector = new MyConnector(config);
    assertEquals("myprefix_", connector.getDocIdPrefix());
    assertEquals("myprefix_id1", connector.createDocId("id1"));
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

}
