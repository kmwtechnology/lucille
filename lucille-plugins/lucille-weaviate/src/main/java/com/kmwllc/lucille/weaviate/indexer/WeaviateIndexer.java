package com.kmwllc.lucille.weaviate.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Runner.RunType;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.auth.exception.AuthException;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.batch.model.ObjectsGetResponseAO2Result.ErrorResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import io.weaviate.client.v1.misc.model.Meta;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeaviateIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(WeaviateIndexer.class);

  private final WeaviateClient client;

  // the name of the object class in the Weviate schema that we are creating or updating
  private final String weaviateClassName;

  // "id" is a reserved property in Weaviate and it must be UUID
  // this indexer creates UUIDs based on the document's original ID
  // we store the document's original ID under an alternate name, specified via idDestinationName
  private final String idDestinationName;

  public WeaviateIndexer(Config config, IndexerMessageManager manager, String metricsPrefix) {
    super(config, manager, metricsPrefix);

    this.weaviateClassName = config.hasPath("weaviate.className") ? config.getString("weaviate.className") : "Document";
    this.idDestinationName = config.hasPath("weaviate.idDestinationName") ? config.getString("weaviate.idDestinationName") :
        "id_original";
    io.weaviate.client.Config weaviateConfig =
        new io.weaviate.client.Config("http", config.getString("weaviate.host"));

    try {
      this.client = WeaviateAuthClient.apiKey(weaviateConfig, config.getString("weaviate.apiKey"));
    } catch (AuthException e) {
      throw new RuntimeException("Couldn't connect to Weaviate instance", e);
    }
  }

  public WeaviateIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, metricsPrefix);
  }

  @Override
  public boolean validateConnection() {
    Result<Meta> meta = client.misc().metaGetter().run();

    if (meta.getError() != null) {
      log.error("Weaviate errors: %s\n", meta.getError().getMessages());
      return false;
    }

    log.info(String.format("Weaviate meta.hostname: %s", meta.getResult().getHostname()));
    log.info(String.format("Weaviate meta.version: %s", meta.getResult().getVersion()));
    log.info(String.format("Weaviate meta.modules: %s", meta.getResult().getModules()));

    return true;
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {

    ObjectsBatcher batcher = client.batch().objectsBatcher();

    for (Document doc : documents) {

      String id = doc.getId();
      Map docMap = doc.asMap();
      docMap.remove(Document.ID_FIELD);
      docMap.put("id_original", id);

      WeaviateObject obj = WeaviateObject.builder()
          .className(weaviateClassName)
          .id(UUID.nameUUIDFromBytes(id.getBytes()).toString())
          .properties(docMap)
          .build();
      batcher.withObject(obj);
    }

    Result<ObjectGetResponse[]> result = batcher.withConsistencyLevel(ConsistencyLevel.ALL).run();

    // result.hasErrors() may return false even if there are errors inside individual ObjectGetResponses
    if (result.hasErrors()) {
      throw new IndexerException(result.getError().toString());
    }

    // examine the responses for each object, looking for errors
    ObjectGetResponse[] responses = result.getResult();
    for (ObjectGetResponse response : responses) {
      ErrorResponse errorResponse = response.getResult().getErrors();
      if (errorResponse != null) {
        // we fail the batch on the first error encountered
        throw new IndexerException(errorResponse.toString());
      }
    }
  }

  @Override
  public void closeConnection() {
    // WeaviateClient as of 4.3.0 does not appear to be closeable
  }

  // a quick way to test the WeaviateIndexer with an example configuration
  // if running in Intellij, set up the Intellij Run/Debug Configuration so:
  // 1) "-cp" is set to lucille-weaviate
  // 2) under "Modify Options" you have selected "add dependencies with 'provided' scope to classpath"
  public static void main(String[] args) throws Exception {
    File file = new File("lucille-plugins/lucille-weaviate/conf-example/application.conf");
    Config config = ConfigFactory.parseFile(file);
    RunResult result = Runner.run(config, RunType.LOCAL);
    System.out.println(result);
  }
}
