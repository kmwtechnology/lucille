package com.kmwllc.lucille.indexer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.BulkIndexByScrollFailure;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: upgrade the ElasticsearchIndexer to use the Elasticsearch Java API Client

/**
 * Indexes documents to Elasticsearch using the Java Client.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>index (String, Required) : Target Elasticsearch index name.</li>
 *   <li>url (String, Required) : Elasticsearch HTTP endpoint (e.g., https://localhost:9200).</li>
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace. Defaults to false.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 *   <li>indexer.routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>indexer.versionType (String, Optional) : Versioning type when using external versions.</li>
 *   <li>elasticsearch.join.joinFieldName (String, Optional) : Name of the join field mapped in the index.</li>
 *   <li>elasticsearch.join.isChild (Boolean, Optional) : Whether documents are children in the join relation. Defaults to false.</li>
 *   <li>elasticsearch.join.childName (String, Optional) : Child relation name when isChild is true.</li>
 *   <li>elasticsearch.join.parentDocumentIdSource (String, Optional) : Source field for the parent document id when isChild is true.</li>
 *   <li>elasticsearch.join.parentName (String, Optional) : Parent relation name when isChild is false.</li>
 * </ul>
 */
public class ElasticsearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index", "url")
      .optionalBoolean("update", "acceptInvalidCert")
      .optionalString("parentName")
      .optionalParent("join", new TypeReference<Map<String, String>>() {}).build();

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchIndexer.class);

  private final ElasticsearchClient client;
  private final String index;

  //flag for using partial update API when sending documents to elastic
  private final boolean update;
  private final ElasticJoinData joinData;
  private final String routingField;
  private final VersionType versionType;

  public ElasticsearchIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix, String localRunId, ElasticsearchClient client) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException(
          "Cannot create ElasticsearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by ElasticsearchIndexer.");
    }
    this.client = client;
    this.index = ElasticsearchUtils.getElasticsearchIndex(config);
    this.update = config.hasPath("elasticsearch.update") ? config.getBoolean("elasticsearch.update") : false;

    joinData = ElasticJoinData.fromConfig(config);
    this.routingField = ConfigUtils.getOrDefault(config, "indexer.routingField", null);
    this.versionType = config.hasPath("indexer.versionType") ? VersionType.valueOf(config.getString("indexer.versionType")) : null;
  }

  public ElasticsearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(config, messenger, bypass, metricsPrefix, localRunId, getClient(config, bypass));
  }

  // Convenience Constructors
  public ElasticsearchIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, ElasticsearchClient client) {
    this(config, messenger, false, metricsPrefix, null, client);
  }

  @Override
  protected String getIndexerConfigKey() { return "elasticsearch"; }

  private static ElasticsearchClient getClient(Config config, boolean bypass) throws IndexerException{
    try {
      return bypass ? null : ElasticsearchUtils.getElasticsearchOfficialClient(config);
    } catch (Exception e) {
      throw new IndexerException("Couldn't create Elasticsearch client", e);
    }
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
      return true;
    }
    BooleanResponse response;
    try {
      response = client.ping();
    } catch (Exception e) {
      log.error("Couldn't ping Elasticsearch ", e);
      return false;
    }
    if (response == null || !response.value()) {
      log.error("Non true response when pinging Elasticsearch: " + response);
      return false;
    }
    return true;
  }

  @Override
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (bypass) {
      return Set.of();
    }

    Map<String, Document> documentsToUpload = new LinkedHashMap<>();
    Set<String> idsToDelete = new LinkedHashSet<>();
    Map<String, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();

    // populate which collection each document belongs to
    // upload if document is not marked for deletion
    // else if document is marked for deletion ONLY, then only add to idsToDelete
    // else document is marked for deletion AND contains deleteByFieldField and deleteByFieldValue, only add to termsToDeleteByQuery
    for (Document doc : documents) {
      // use doc id override if specified, otherwise delete will try to delete wrong id
      String id = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());

      if (!isMarkedForDeletion(doc)) {
        idsToDelete.remove(id);
        documentsToUpload.put(id, doc);
      } else {
        documentsToUpload.remove(id);
        if (!isMarkedForDeletionByField(doc)) {
          idsToDelete.add(id);
        } else {
          //indexer.deleteByFieldField gives you the field in the document that holds which field whose value is queried for
          // deletion, so we need to do doc.getString(deleteByFieldField).
          String deleteField = doc.getString(deleteByFieldField);
          if (!termsToDeleteByQuery.containsKey(deleteField)) {
            termsToDeleteByQuery.put(deleteField, new ArrayList<>());
          }
          termsToDeleteByQuery.get(deleteField).add(doc.getString(deleteByFieldValue));
        }
      }
    }

    Set<Pair<Document, String>> failedDocs = uploadDocuments(documentsToUpload.values());
    deleteById(new ArrayList<>(idsToDelete));
    deleteByQuery(termsToDeleteByQuery);

    return failedDocs;
  }

  private void deleteById(List<String> idsToDelete) throws Exception {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    for (String id : idsToDelete) {
      br.operations(op -> op
          .delete(d -> d
              .index(index)
              .id(id)
          )
      );
    }

    BulkResponse response = client.bulk(br.build());
    if (response.errors()) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        }
      }
      throw new IndexerException("encountered errors while deleting documents");
    }
  }

  private void deleteByQuery(Map<String, List<String>> termsToDeleteByQuery) throws Exception {
    if (termsToDeleteByQuery.isEmpty()) {
      return;
    }
    /*
      each entry from termsToDeleteByQuery would add a "terms" query to the should clause. In each entry, the key represents
      the field to look for while querying and values represent the values to look for in that field. We set the
      minimum should match to 1 such that any documents that passes a single terms query would end
      up in the result set. For a document to pass a terms query, it must contain that field and in that field
      contain any one of the field values.
      e.g. termsToDeleteByQuery -> {
                                      "field1" : ["value1", "value2", "value3"],
                                      "field2" : ["valueA", "valueB"]
                                   }
      would create a delete query:
      {
        "query": {
          "bool": {
            "should": [
              {
                "terms": {
                  "field1": ["value1", "value2", "value3"]
                }
              },
              {
                "terms": {
                  "field2": ["valueA", "valueB"]
                }
              }
            ],
            "minimum_should_match": "1"
          }
        }
      }
     */

    BoolQuery.Builder boolQuery = new BoolQuery.Builder();

    for (Map.Entry<String, List<String>> entry : termsToDeleteByQuery.entrySet()) {
      String field = entry.getKey();
      List<String> values = entry.getValue();
      boolQuery.should(s -> s
          .terms(t -> t
              .field(field)
              .terms(tt -> tt.value(values.stream()
                  .map(FieldValue::of)
                  .collect(Collectors.toList()))
              )
          )
      );
    }

    boolQuery.minimumShouldMatch("1");

    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest.Builder()
        .index(index)
        .query(q -> q.bool(boolQuery.build()))
        .build();
    DeleteByQueryResponse response = client.deleteByQuery(deleteByQueryRequest);

    if (!response.failures().isEmpty()) {
      for (BulkIndexByScrollFailure failure : response.failures()) {
        log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
      }
      throw new IndexerException("encountered errors while deleting by query");
    }
  }

  private Set<Pair<Document, String>> uploadDocuments(Collection<Document> documentsToUpload) throws IOException, IndexerException {
    if (documentsToUpload.isEmpty()) {
      return Set.of();
    }

    // building a map so we can quickly retrieve documents later on, if they fail during indexing.
    Map<String, Document> documentsUploaded = new LinkedHashMap<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Document doc : documentsToUpload) {
      // populate join data to document
      joinData.populateJoinData(doc);

      // removing fields in the blacklist or not in the whitelist in configurations
      Map<String, Object> indexerDoc = getIndexerDoc(doc);

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      documentsUploaded.put(docId, doc);

      // only add id if our fieldFilter allows it (based on our blacklist and whitelist)
      // - Case 1: id and idOverride filtered out -> idOverride used by Indexer, both removed from Document (tested in testBlacklistWithOverride)
      // - Case 2: id is filtered, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testBlacklistWithOverride2)
      // - Case 3: id is filtered, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
      // - Case 4: both unfiltered, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
      // - Case 5: both unfiltered, idOverride null -> document id remains and used by Indexer (Default case & tested)
      if (fieldFilter.shouldInclude(Document.ID_FIELD)) {
        indexerDoc.put(Document.ID_FIELD, docId);
      }

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);

      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? ((KafkaDocument) doc).getOffset()
          : null;

      if (update) {
        br.operations(op -> op
            .update(updateBuilder -> {

              if (routingField != null) {
                updateBuilder.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                updateBuilder.versionType(versionType).version(versionNum);
              }

              return updateBuilder
                  .id(docId)
                  .index(index)
                  .action(upx -> upx
                      .doc(indexerDoc)
                  );
            }));
      } else {
        br.operations(op -> op
            .index(operationBuilder -> {

                  if (routingField != null) {
                    operationBuilder.routing(doc.getString(routingField));
                  }
                  if (versionNum != null) {
                    operationBuilder.versionType(versionType).version(versionNum);
                  }

                  return operationBuilder
                      .id(docId)
                      .index(index)
                      .document(indexerDoc);
                }
            ));
      }
    }

    Set<Pair<Document, String>> failedDocs = new HashSet<>();

    BulkResponse response = client.bulk(br.build());
    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          // For the error - if the id is a document's id, then it failed, and we add it to the set.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (documentsUploaded.containsKey(item.id())) {
            Document failedDoc = documentsUploaded.get(item.id());
            failedDocs.add(Pair.of(failedDoc, item.error().reason()));
          } else {
            throw new IndexerException(item.error().reason());
          }
        }
      }
    }

    return failedDocs;
  }

  private boolean isMarkedForDeletion(Document doc) {
    return deletionMarkerField != null
        && deletionMarkerFieldValue != null
        && doc.hasNonNull(deletionMarkerField)
        && doc.getString(deletionMarkerField).equals(deletionMarkerFieldValue);
  }

  private boolean isMarkedForDeletionByField(Document doc) {
    return deleteByFieldField != null
        && doc.has(deleteByFieldField)
        && deleteByFieldValue != null
        && doc.has(deleteByFieldValue);
  }

  @Override
  public void closeConnection() {
    if (!bypass && client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing ElasticsearchClient", e);
      }
    }
  }

  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    List<Document> children = doc.getChildren();
    if (children == null || children.isEmpty()) {
      return;
    }
    for (Document child : children) {
      Map<String, Object> map = child.asMap();
      Map<String, Object> indexerChildDoc = new HashMap<>();
      for (String key : map.keySet()) {
        // we don't support children that contain nested children
        if (Document.CHILDREN_FIELD.equals(key)) {
          continue;
        }
        Object value = map.get(key);
        indexerChildDoc.put(key, value);
      }
      // TODO: Do nothing for now, add support for child docs like SolrIndexer does in future (_childDocuments_)
    }
  }

  public static class ElasticJoinData {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String joinFieldName;
    private boolean isChild;
    private String parentName;
    private String childName;
    private String parentDocumentIdSource;

    public void populateJoinData(Document doc) {

      // no need to do a join
      if (joinFieldName == null) {
        return;
      }

      // check that join field doesn't already exist
      if (doc.has(joinFieldName)) {
        throw new IllegalStateException("Document already has join field: " + joinFieldName);
      }

      // set a necessary join field
      if (isChild) {
        String parentId = doc.getString(parentDocumentIdSource);
        doc.setField(joinFieldName, getChildNode(parentId));
      } else {
        doc.setField(joinFieldName, parentName);
      }
    }

    private JsonNode getChildNode(String parentId) {
      return MAPPER.createObjectNode()
          .put("name", childName)
          .put("parent", parentId);
    }

    private static String prefix() {
      return prefix(null);
    }

    private static String prefix(String path) {
      // todo consider abstracting adding prefix to config
      StringBuilder builder = new StringBuilder("elasticsearch.join");
      if (path == null || path.isEmpty()) {
        return builder.toString();
      }
      return builder.append(".").append(path).toString();
    }

    public static ElasticJoinData fromConfig(Config config) {


      ElasticJoinData data = new ElasticJoinData();

      // if no join in config will initialize all join data to null and will be skipped in the code
      if (config.hasPath(prefix())) {

        // set fields for both parent and child
        data.joinFieldName = config.getString(prefix("joinFieldName"));
        data.isChild = ConfigUtils.getOrDefault(config, prefix("isChild"), false);

        // set parent child specific fields
        if (data.isChild) {
          data.childName = config.getString(prefix("childName"));
          data.parentDocumentIdSource = config.getString(prefix("parentDocumentIdSource"));
        } else {
          data.parentName = config.getString(prefix("parentName"));
        }
      }

      return data;
    }
  }

}
