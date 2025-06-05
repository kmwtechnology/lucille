package com.kmwllc.lucille.weaviate.indexer;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Runner.RunType;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.message.IndexerMessenger;
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
import io.weaviate.client.v1.data.model.WeaviateObject.WeaviateObjectBuilder;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import io.weaviate.client.v1.misc.model.Meta;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeaviateIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(WeaviateIndexer.class);

  private final WeaviateClient client;

  // the name of the object class in the Weaviate schema that we are creating or updating
  private final String weaviateClassName;

  // "id" is a reserved property in Weaviate, and it must be UUID
  // this indexer creates UUIDs based on the document's original ID
  // we store the document's original ID under an alternate name, specified via idDestinationName
  private final String idDestinationName;

  private final String vectorField;

  public WeaviateIndexer(Config config, IndexerMessenger messenger, WeaviateClient client,
      String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId, Spec.indexer()
        .withRequiredProperties("apiKey", "host")
        .withOptionalProperties("className", "idDestinationName", "vectorField"));

    this.weaviateClassName = config.hasPath("weaviate.className") ? config.getString("weaviate.className") : "Document";
    this.idDestinationName = config.hasPath("weaviate.idDestinationName") ? config.getString("weaviate.idDestinationName") :
        "id_original";

    this.vectorField = ConfigUtils.getOrDefault(config, "weaviate.vectorField", null);
    this.client = client;
  }

  public WeaviateIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this(config, messenger, getClient(config, bypass), metricsPrefix, localRunId);
  }

  // Convenience Constructors
  public WeaviateIndexer(Config config, IndexerMessenger messenger, WeaviateClient client, String metricsPrefix) {
    this(config, messenger, client, metricsPrefix, null);
  }

  public WeaviateIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, getClient(config, bypass), metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "weaviate"; }

  private static WeaviateClient getClient(Config config, boolean bypass) {
    if (bypass) {
      return null;
    }

    // todo when parsing host, should we check for http/https and port? or be able not to provide it at all?
    io.weaviate.client.Config weaviateConfig =
        new io.weaviate.client.Config("https", config.getString("weaviate.host"), null, 6000, 6000, 6000);

    try {
      return WeaviateAuthClient.apiKey(weaviateConfig, config.getString("weaviate.apiKey"));
    } catch (AuthException e) {
      throw new RuntimeException("Couldn't connect to Weaviate instance", e);
    }
  }

  @Override
  public boolean validateConnection() {
    Result<Meta> meta = client.misc().metaGetter().run();

    if (meta.getError() != null) {
      log.error("Weaviate errors: {}\n", meta.getError().getMessages());
      return false;
    }

    log.info("Weaviate meta.hostname: {}", meta.getResult().getHostname());
    log.info("Weaviate meta.version: {}", meta.getResult().getVersion());
    log.info("Weaviate meta.modules: {}", meta.getResult().getModules());

    return true;
  }

  @Override
  protected Set<Document> sendToIndex(List<Document> documents) throws Exception {
    Set<Document> failedDocs = new HashSet<>();

    try (ObjectsBatcher batcher = client.batch().objectsBatcher()) {
      Map<String, Document> docGeneratedUUIDMap = new HashMap<>();

      for (Document doc : documents) {
        // initialize the docMap with all the fields from the document and set the id destination field instead of id field
        Map<String, Object> docMap = doc.asMap();
        docMap.remove(Document.ID_FIELD);
        docMap.put(idDestinationName, doc.getId());

        // set id and class name
        String uuidForDoc = generateDocumentUUID(doc);
        WeaviateObjectBuilder objectBuilder = WeaviateObject.builder()
            .className(weaviateClassName)
            .id(uuidForDoc);

        // if vector field is specified set it and remove it from the docMap
        if (vectorField != null && doc.has(vectorField)) {
          objectBuilder.vector(floatsToArray(doc.getFloatList(vectorField)));
          docMap.remove(vectorField);
        }

        // set properties and build object
        WeaviateObject obj = objectBuilder
            .properties(docMap)
            .build();
        batcher.withObject(obj);

        docGeneratedUUIDMap.put(uuidForDoc, doc);
      }

      Result<ObjectGetResponse[]> result = batcher.withConsistencyLevel(ConsistencyLevel.ALL).run();

      // result.hasErrors() is indicative of a larger API / request failure.
      // It can return false (no errors), even when some individual ObjectGetResponses will have errors.
      if (result.hasErrors()) {
        throw new IndexerException(result.getError().toString());
      }

      // examine the responses for each object, looking for errors
      ObjectGetResponse[] responses = result.getResult();
      for (ObjectGetResponse response : responses) {
        ErrorResponse errorResponse = response.getResult().getErrors();

        if (errorResponse != null) {
          Document docWithResponseUUID = docGeneratedUUIDMap.get(response.getId());
          failedDocs.add(docWithResponseUUID);
        }
      }
    } catch (Exception e) {
      throw new IndexerException("Error occurred sending Documents to Weaviate:", e);
    }

    return failedDocs;
  }

  public static String generateDocumentUUID(Document document) {
    return UUID.nameUUIDFromBytes(document.getId().getBytes()).toString();
  }

  private static Float[] floatsToArray(List<Float> list) {
    if (list == null) {
      throw new IllegalArgumentException("expecting a non empty list of floats");
    }
    int size = list.size();
    Float[] array = new Float[size];
    for (int i = 0; i < size; i++) {
      array[i] = list.get(i);
    }
    return array;
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
