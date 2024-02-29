package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.endpoints.BooleanResponse;


public class ElasticsearchLookupTest {
  private StageFactory factory = StageFactory.of(ElasticsearchLookup.class);

  @Test
  public void testMalformedConfigs() throws Exception {
    Config one = ConfigFactory.load("ElasticSearchLookupTest/noSource.conf");
    Config two = ConfigFactory.load("ElasticSearchLookupTest/noDest.conf");
    Config three = ConfigFactory.load("ElasticSearchLookupTest/noUrl.conf");
    Config four = ConfigFactory.load("ElasticSearchLookupTest/noIndex.conf");
    assertThrows(StageException.class, () -> new ElasticsearchLookup(one));
    assertThrows(StageException.class, () -> new ElasticsearchLookup(two));
    assertThrows(StageException.class, () -> new ElasticsearchLookup(three));
    assertThrows(StageException.class, () -> new ElasticsearchLookup(four));
  }

  @Test 
  public void testCorrectExceptions() throws Exception {
    Config config = ConfigFactory.empty("one");
    config = ConfigFactory.load("ElasticsearchLookupTest/basic.conf");
    Config config2 = ConfigFactory.empty("two");
    Config config3 = ConfigFactory.empty("three");
    Config config4 = ConfigFactory.empty("four");
    try (MockedStatic<ElasticsearchUtils> mockedUtils = Mockito.mockStatic(ElasticsearchUtils.class)) {
      ElasticsearchClient noPing = mock(ElasticsearchClient.class);
      ElasticsearchClient nullPing = mock(ElasticsearchClient.class);
      ElasticsearchClient falsePing = mock(ElasticsearchClient.class);
      
      when(nullPing.ping()).thenReturn(null);
      when(falsePing.ping()).thenReturn(new BooleanResponse(false));

      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config)).thenReturn(null);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config2)).thenReturn(noPing);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config3)).thenReturn(nullPing);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config4)).thenReturn(falsePing);

      
    }
  }
}
