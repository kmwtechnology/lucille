package com.kmwllc.lucille.stage;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ElasticsearchLookup extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchLookup.class);

  private ElasticsearchClient client;
  private final String index;

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final UpdateMode updateMode;

  public ElasticsearchLookup(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("update_mode")
        .withRequiredProperties("source", "dest", "elasticsearch.url", "elasticsearch.index")
        .withOptionalProperties("elasticsearch.acceptInvalidCert", "update_mode"));

    this.client = ElasticsearchUtils.getElasticsearchOfficialClient(config);
    this.index = ElasticsearchUtils.getElasticsearchIndex(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    BooleanResponse response;
    if (client == null) {
      throw new StageException("Client was not created.");
    }
    try {
      response = client.ping();
    } catch (Exception e) {
      throw new StageException("Couldn't ping elasticsearch", e);
    }
    if (response == null || !response.value()) {
      throw new StageException("Non true response when pinging Elasticsearch: " + response);
    }
  }

  @Override
  public void stop() throws StageException {
    if (client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing ElasticsearchClient", e);
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      GetResponse<ObjectNode> response = client.get(g -> g
              .index(index)
              .id(doc.getId()),
          ObjectNode.class);

      if (response.found()) {
        ObjectNode json = response.source();
        for (int i = 0; i < sourceFields.size(); i++) {
          JsonNode node = json.get(sourceFields.get(i));
          if (node == null) {
             continue;
          }
          doc.update(destFields.get(i), updateMode, node.asText());
        }
      }
      return null;
    } catch (IOException e) {
      throw new StageException(String.format("Error looking up fields in elasticsearch for doc id: %s", doc.getId()), e);
    }
  }
}
