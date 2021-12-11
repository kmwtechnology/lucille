package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import org.opensearch.client.base.BooleanResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._global.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OpenSearchIndexer extends Indexer {

    private static final Logger log = LoggerFactory.getLogger(SolrIndexer.class);

    private final OpenSearchClient client;
    private final String index;

    public OpenSearchIndexer(Config config, IndexerMessageManager manager, OpenSearchClient client, String metricsPrefix) {
        super(config, manager, metricsPrefix);
        this.client = client;
        this.index = OpenSearchUtils.getOpenSearchIndex(config);
    }

    public OpenSearchIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
        this(config, manager, getClient(config, bypass), metricsPrefix);
    }

    private static OpenSearchClient getClient(Config config, boolean bypass) {
        return bypass ? null : OpenSearchUtils.getOpenSearchClient(config);
    }

    @Override
    public boolean validateConnection() {
        if (client==null) {
            return true;
        }
        BooleanResponse response;
        try {
            response = client.ping();
        } catch (Exception e) {
            log.error("Couldn't ping OpenSearch ", e);
            return false;
        }
        if (response==null) {
            log.error("Null response when pinging OpenSearch");
            return false;
        }
        if (!response.value()) {
            log.error("Non true response when pinging OpenSearch: " + response);
        }
        return true;
    }

    @Override
    public void closeConnection() {
        // TODO: Connections don't seem persistent, nothing to close, revisit with non-beta opensearch-java client
    }

    @Override
    protected void sendToIndex(List<Document> documents) throws Exception {
        // skip indexing if there is no indexer client
        if (client == null) return;

        // determine what field to use as id field and iterate over the documents
        //String idField = Optional.ofNullable(get).orElse(Document.ID_FIELD);
        for (Document doc : documents) {
            Map<String,Object> indexerDoc = doc.asMap();

            // remove children documents field from indexer doc (processed from doc by addChildren method call below)
            indexerDoc.remove(Document.CHILDREN_FIELD);

            // if a doc id override value exists, make sure it is used instead of pre-existing doc id
            String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
            indexerDoc.put(Document.ID_FIELD, docId);


//            for (String key : map.keySet()) {
//
//                // skip child document fields for processing below
//                if (Document.CHILDREN_FIELD.equals(key)) continue;
//
//                // replace document id with override if present
//                if (idOverride!=null && Document.ID_FIELD.equals(key)) {
//                    indexerDoc.put(Document.ID_FIELD, idOverride);
//                    continue;
//                }
//
//                Object value = map.get(key);
//                indexerDoc.put(key, value);
//            }

            // handle special operations required to add children documents
            addChildren(doc, indexerDoc);

            // TODO: Modify this to support adding documents in batches once opensearch-java client supports it
//            String docId = (String) indexerDoc.get(Document.ID_FIELD);
            IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                    .index(index).id(docId).value(indexerDoc).build();
            client.index(indexRequest);
        }
    }

    private void addChildren(Document doc, Map<String, Object> indexerDoc) {
        List<Document> children = doc.getChildren();
        if (children==null || children.isEmpty()) {
            return;
        }
        for (Document child : children) {
            Map<String,Object> map = child.asMap();
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

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.loadConfig();
        String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
        log.info("Starting Indexer for pipeline: " + pipelineName);
        IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
        Indexer indexer = new OpenSearchIndexer(config, manager, false, pipelineName);
        if (!indexer.validateConnection()) {
            log.error("Indexer could not connect");
            System.exit(1);
        }

        Thread indexerThread = new Thread(indexer);
        indexerThread.start();

        Signal.handle(new Signal("INT"), signal -> {
            indexer.terminate();
            log.info("Indexer shutting down");
            try {
                indexerThread.join();
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
            System.exit(0);
        });
    }

}