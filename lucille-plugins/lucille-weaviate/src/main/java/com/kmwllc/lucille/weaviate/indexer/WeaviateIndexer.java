package com.kmwllc.lucille.weaviate.indexer;

import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@AutoService(Indexer.class)
@IndexerType("Weaviate")
public class WeaviateIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("apiKey", "host")
      .optionalString("className", "idDestinationName", "vectorField").build();

  private static final Logger log = LoggerFactory.getLogger(WeaviateIndexer.class);

  private final WeaviateClient client;

  // the name of the object class in the Weaviate schema that we are creating or updating
  private final String weaviateClassName;

  // "id" is a reserved property in Weaviate, and it must be UUID
  // this indexer creates UUIDs based on the document's original ID
  // we store the document's original ID under an alternate name, specified via idDestinationName
  private final String idDestinationName;

  private final String vectorField;

  public WeaviateIndexer(Config config, IndexerContext context, WeaviateClient client) {
    super(config, context);

    this.weaviateClassName = ConfigUtils.getStringOrDefault(config, "weaviate.className", "Document");
    this.idDestinationName = ConfigUtils.getStringOrDefault(config, "weaviate.idDestinationName", "id_original");

    this.vectorField = ConfigUtils.getStringOrDefault(config, "weaviate.vectorField", null);
    this.client = client;
  }

  public WeaviateIndexer(Config config, IndexerContext context) {
    this(config, context, getClient(config, context.bypass()));
  }

  @Override
  protected String getIndexerConfigKey() { return "weaviate"; }

  // a document marked for deletion would otherwise be indexed
  @Override
  protected Set<String> getUnsupportedSettings() {
    return Set.of("indexOverrideField", "deletionMarker", "deleteByQuery");
  }

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
  protected boolean checkConnection() {
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
  protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
    List<Document> documents = send.documents();
    List<IndexerFailure> failedDocs = new ArrayList<>();

    try (ObjectsBatcher batcher = client.batch().objectsBatcher()) {
      // several documents can share a UUID, as it comes from the id or id override, and each gets its own response
      Map<String, Deque<Document>> docsByGeneratedUUID = new HashMap<>();

      for (Document doc : documents) {
        // the document's fields with indexer.includeFields / indexer.excludeFields applied; the id moves to idDestinationName
        // (indexer.idOverrideField, when configured and present, supplies the id written to Weaviate)
        Map<String, Object> docMap = getConvertedIndexerDoc(doc);
        docMap.remove(Document.ID_FIELD);
        String destinationId = getDestinationId(doc);
        docMap.put(idDestinationName, destinationId);

        // set id and class name
        String uuidForDoc = generateDocumentUUID(destinationId);
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

        docsByGeneratedUUID.computeIfAbsent(uuidForDoc, uuid -> new ArrayDeque<>()).add(doc);
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
        Deque<Document> docsWithResponseUUID = docsByGeneratedUUID.get(response.getId());
        Document docWithResponseUUID = docsWithResponseUUID == null ? null : docsWithResponseUUID.poll();

        if (errorResponse != null) {
          failedDocs.add(new IndexerFailure(docWithResponseUUID, new IndexerException(errorResponse.toString())));
        }
      }
    } catch (Exception e) {
      throw new IndexerException("Error occurred sending Documents to Weaviate:", e);
    }

    return failedDocs;
  }

  public static String generateDocumentUUID(Document document) {
    return generateDocumentUUID(document.getId());
  }

  public static String generateDocumentUUID(String id) {
    return UUID.nameUUIDFromBytes(id.getBytes()).toString();
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
}
