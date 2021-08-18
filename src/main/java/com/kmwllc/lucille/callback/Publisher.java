package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides a way to "publish" documents for processing by the pipeline. Maintains an internal accounting of document
 * status so that it is possible tell when all published documents and their children have been indexed. Provides
 * a way to update this accounting based on received Events.
 *
 * A new Publisher should be created for each "run" of a sequence of Connectors. The Publisher is responsible
 * for stamping a designated run_id on each published Document and maintaining accounting details specific to that run.
 */
public class Publisher {

  public static final Logger log = LoggerFactory.getLogger(Publisher.class);

  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;

  private final String runId;

  private int numPublished = 0;

  // List of published documents that are not yet indexed. Also includes children of published documents.
  // Note that this is a List, not a Set, because if two documents with the same ID are published, we would
  // expect to receive two separate INDEX events relating to those documents, and we will therefore make
  // two attempts to remove the ID. Upon each removal attempt, we would like there to be something present
  // to remove; otherwise we would classify the event as an "early" INDEX event and treat it specially.
  // Also note that a Publisher may be shared by a Runner and a Connector: the connector may be publishing
  // new Documents while the Connector is receiving Events and calling handleEvent().
  // publish() and handleEvent() both update docIdsToTrack so the list should be synchronized.
  // TODO: review whether publish() and handleEvent() should themselves be synchronized.
  private List<String> docIdsToTrack = Collections.synchronizedList(new ArrayList<String>());

  // List of child documents for which an INDEX event has been received early, before the corresponding CREATE event
  private List<String> docIdsIndexedBeforeTracking = Collections.synchronizedList(new ArrayList<String>());

  public Publisher(String runId) {
    this.runId = runId;
    this.kafkaProducer = KafkaUtils.createProducer();
  }

  /**
   * Submits the given document for processing by any available pipeline worker.
   *
   * Stamps the current Run ID on the document and begins "tracking" events relating the document.
   */
  public void publish(Document document) throws Exception {
    document.setField("run_id", runId);
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.sourceTopic"), document.getId(), document.toString())).get();
    log.info("Published: " + result);
    kafkaProducer.flush();
    docIdsToTrack.add(document.getId());
    numPublished++;
  }

  /**
   * Returns the number of documents published so far.
   *
   */
  public int numPublished() {
    return numPublished;
  }

  /**
   * Closes the any connections opened by the publisher (e.g. a KafkaProducer).
   *
   */
  public void close() throws Exception {
    kafkaProducer.close();
  }

  /**
   * Updates internal accounting based on a received Event: if we learn that a document has
   * been indexed, we can stop tracking it. If we learn that a child document has been
   * newly created, we must start tracking it. If we learn that a child document has been
   * indexed before we began tracking it (i.e. before we received its CREATE event) we need
   * to store its ID separately so that when the CREATE event is eventually received,
   * we'll know NOT to begin tracking it.
   *
   * @param event the Event to handle
   */
  public void handleEvent(Event event) {
    String docId = event.getDocumentId();

    if (event.isCreate()) {
      if (!docIdsIndexedBeforeTracking.remove(docId)) {
        docIdsToTrack.add(docId);
      }
    } else {
      if (!docIdsToTrack.remove(docId)) {
        docIdsIndexedBeforeTracking.add(docId);
      }
    }
  }

  /**
   * Returns true if the publisher is not expecting any Events relating to the
   * documents it has published. This will be the case if:
   *
   * 1) an INDEX event has been received for all Documents published via Publisher.publish(),
   * 2) an INDEX event has been received for all children Documents that the Publisher was made aware of
   * via a CREATE event
   * 3) a CREATE event has been received for any "early" INDEX events that were received before the publisher
   * began tracking the given child document
   *
   */
  public boolean hasReceivedAllExepectedEvents() {
    return docIdsToTrack.isEmpty() && docIdsIndexedBeforeTracking.isEmpty();
  }

  /**
   * Returns the number of documents for which we are still awating an INDEX event.
   */
  public int countExpectedIndexEvents() {
    return docIdsToTrack.size();
  }

  /**
   * Returns the number of documents for which an INDEX event was received prior to the corresponding CREATE event.
   */
  public int countExpectedCreateEvents() {
    return docIdsIndexedBeforeTracking.size();
  }

}
