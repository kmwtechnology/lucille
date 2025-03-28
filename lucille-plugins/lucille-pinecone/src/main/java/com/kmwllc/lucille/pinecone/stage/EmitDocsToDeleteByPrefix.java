package com.kmwllc.lucille.pinecone.stage;

import com.kmwllc.lucille.core.StageSpec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.pinecone.util.PineconeUtils;
import com.typesafe.config.Config;
import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.ListItem;
import io.pinecone.proto.ListResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This stage collects all records from namespace(s) if their prefix ID matches the ID of the Lucille document marked for deletion.
 * These records are then emitted as Lucille Documents, which are also marked for deletion.
 *
 * Config Parameters:
 * - dropOriginal (Boolean, Optional): if set to true, will drop documents in the pipeline that were marked for deletion
 *   as dropped. Does not apply to emitted documents, only the original documents. Defaults to false
 * - addPrefix (String, optional) : String that adds on to all document ids used for listing
 *    e.g. if id of doc is "doc1" and addPrefix is "-v1", then this stage will retrieve and emit documents where their
 *         id prefix is "doc1-v1".
 * - index (String) : name of the index to perform operation on.
 * - apiKey (String) : apiKey used to create the client.
 * - namespaces (Map&lt;String, Object&gt;, Optional) : mapping of namespaces of which you would like to collect ids on. If not given, will
 *    retrieve from default namespace.
 *   - note that value of mapping will not matter, rather intended to make it convenient for user to reuse same namespaces configs as
 *     PineconeIndexer
 * - deletionMarkerField (String) : name of the field which indicates the document is marked for deletion
 * - deletionMarkerFieldValue (String) : value of the field which indicates that the document is marked for deletion
 *   - for stage to work as intended, index, apiKey, namespaces, deletionMarkerField and deletionMarkerFieldValue
 *     should have the same value provided in indexer configs
 */
public class EmitDocsToDeleteByPrefix extends Stage {

  private Pinecone client;
  private final String indexName;
  private Index index;
  private final Map<String, Object> namespaces;
  private final boolean dropOriginal;
  private final String addPrefix;
  private final String deletionMarkerField;
  private final String deletionMarkerFieldValue;

  public EmitDocsToDeleteByPrefix(Config config) {
    super(config, new StageSpec()
        .withOptionalProperties("dropOriginal", "addPrefix")
        .withOptionalParents("namespaces")
        .withRequiredProperties("apiKey", "deletionMarkerField", "deletionMarkerFieldValue", "index"));
    this.indexName = config.getString("index");
    this.namespaces = config.hasPath("namespaces") ? config.getConfig("namespaces").root().unwrapped() : null;
    this.dropOriginal = config.hasPath("dropOriginal") ? config.getBoolean("dropOriginal") : false;
    this.addPrefix = config.hasPath("addPrefix") ? config.getString("addPrefix") : "";
    this.deletionMarkerField = config.getString("deletionMarkerField");
    this.deletionMarkerFieldValue = config.getString("deletionMarkerFieldValue");
  }

  @Override
  public void start() throws StageException {
    try {
      client = new Pinecone.Builder(config.getString("apiKey")).build();
      index = client.getIndexConnection(indexName);
    } catch (Exception e) {
      throw new StageException("Unable to connect to client or index.", e);
    }

    if (!PineconeUtils.isClientStable(client, indexName)) {
      throw new StageException("Index " + indexName + " is not stable.");
    }
  }

  @Override
  public void stop() {
    if (index != null) {
      index.close();
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // check if document has been set for deletion and if not skip current document
    if (!isMarkedForDeletion(doc)) {
      return null;
    }

    List<String> idsToDelete = new ArrayList<>();
    // collect all documents that meet this prefix in all namespaces given
    if (namespaces != null) {
      for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
        idsToDelete.addAll(getVectorsByPrefix(doc, addPrefix, entry.getKey()));
      }
    } else {
      idsToDelete.addAll(getVectorsByPrefix(doc, addPrefix, PineconeUtils.getDefaultNamespace()));
    }

    // drop original document if set to true
    if (dropOriginal) {
      doc.setDropped(true);
    }

    if (idsToDelete.isEmpty()) {
      return null;
    } else {
      // create Lucille document for each id collected, mark them for deletion and emit them
      return createDocsToDeleteFromIds(idsToDelete).iterator();
    }
  }

  private List<Document> createDocsToDeleteFromIds(List<String> idsToDelete) {
    List<Document> documents = new ArrayList<>();
    for (String id : idsToDelete) {
      Document newDoc = Document.create(id);
      newDoc.setField(deletionMarkerField, deletionMarkerFieldValue);
      documents.add(newDoc);
    }
    return documents;
  }


  private List<String> getVectorsByPrefix(Document doc, String additionalPrefix, String namespace)
      throws StageException {
    List<ListItem> idsToDelete;
    String prefix = doc.getId() + additionalPrefix;
    try {
      // by default, list will list up to 100 vectors
      ListResponse listResponse = index.list(namespace, prefix);
      idsToDelete = new ArrayList<>(listResponse.getVectorsList());

      // if number of vectors exceed 100
      while (listResponse.hasPagination()) {
        String paginationToken = listResponse.getPagination().getNext();
        listResponse = index.list(namespace, prefix, paginationToken);
        idsToDelete.addAll(listResponse.getVectorsList());
      }
    } catch (Exception e) {
      throw new StageException("Error listing vectors to delete", e);
    }
    return idsToDelete.stream().map(ListItem::getId).collect(Collectors.toList());
  }

  private boolean isMarkedForDeletion(Document doc) {
    return deletionMarkerField != null
        && deletionMarkerFieldValue != null
        && doc.hasNonNull(deletionMarkerField)
        && doc.getString(deletionMarkerField).equals(deletionMarkerFieldValue);
  }
}

