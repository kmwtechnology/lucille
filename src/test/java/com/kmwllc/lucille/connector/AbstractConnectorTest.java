package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AbstractConnectorTest {

  private class MyConnector extends AbstractConnector {

    public MyConnector(Config config) {
      super(config);
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

}
