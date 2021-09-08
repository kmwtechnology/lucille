package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.PublisherMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Publisher implementation that maintains an in-memory list of pending documents.
 *
 * Note that this implementation includes an explicit design decision that the the publisher should
 * not remember all of the documents it has published because there could be an unbounded number of such docs
 * and they could take up significant space in memory. We don't even want the publisher to remember
 * all of the ids of those documents. As soon as the publisher learns that a document has reached
 * an end-state in the workflow, it can "forget" about that document aside from including it
 * a count of how many documents were published. The intention is that the publisher will be receiving
 * events in a separate thread from the one that is publishing documents (e.g. handleEvent() and publish() will
 * be called by separate threads) so that the publisher can stop tracking some documents even as it starts
 * tracking new ones. To achieve a memory of all the documents that were published,
 * a persisting implementation of PublisherMessageManager could be given to the Publisher at construction time,
 * and the history could be accessed from the message manager.
 */
public class PublisherImpl implements Publisher {

  public static final Logger log = LoggerFactory.getLogger(PublisherImpl.class);

  private final PublisherMessageManager manager;

  private final String runId;

  private final String pipelineName;

  private int numPublished = 0;

  // List of published documents that are not yet indexed. Also includes children of published documents.
  // Note that this is a List, not a Set, because if two documents with the same ID are published, we would
  // expect to receive two separate INDEX events relating to those documents, and we will therefore make
  // two attempts to remove the ID. Upon each removal attempt, we would like there to be something present
  // to remove; otherwise we would classify the event as an "early" INDEX event and treat it specially.
  // Also note that a Publisher may be shared by a Runner and a Connector: the connector may be publishing
  // new Documents while the Connector is receiving Events and calling handleEvent().
  // publish() and handleEvent() both update docIdsToTrack so the list should be synchronized.
  private List<String> docIdsToTrack = Collections.synchronizedList(new ArrayList<String>());

  // List of child documents for which an INDEX event has been received early, before the corresponding CREATE event
  private List<String> docIdsIndexedBeforeTracking = Collections.synchronizedList(new ArrayList<String>());

  public PublisherImpl(PublisherMessageManager manager, String runId, String pipelineName) throws Exception {
    this.manager = manager;
    this.runId = runId;
    this.pipelineName = pipelineName;
    manager.initialize(runId, pipelineName);
  }

  @Override
  public void publish(Document document) throws Exception {
    document.setField("run_id", runId);
    manager.sendForProcessing(document);
    docIdsToTrack.add(document.getId());
    numPublished++;
  }

  @Override
  public int numPublished() {
    return numPublished;
  }

  @Override
  public void close() throws Exception {
    manager.close();
  }

  @Override
  public void handleEvent(Event event) {
    String docId = event.getDocumentId();

    // if we're learning that a child document has been created, we need to begin tracking it unless
    // we have already received an early confirmation that it was indexed
    if (event.isCreate()) {
      if (!docIdsIndexedBeforeTracking.remove(docId)) {
        docIdsToTrack.add(docId);
      }
    } else {
      // if we're learning that a document has been indexed, we can stop tracking it;
      // but if we weren't previously tracking it, we need to remember that we've seen it so that
      // if we receive an out-of-order or late create event for this document in the future,
      // we won't start tracking it then
      if (!docIdsToTrack.remove(docId)) {
        docIdsIndexedBeforeTracking.add(docId);
      }
    }
  }

  @Override
  public boolean waitForCompletion(ConnectorThread thread) throws Exception {
    // poll for Events relating the current run; loop until all work is complete
    while (true) {
      Event event = manager.pollEvent();

      if (event !=null) {
        handleEvent(event);
      }

      // stop waiting if the connector threw an exception
      // TODO: consider whether we still want to wait for completion of any work the connector may have generated before it threw the exception
      if (thread.hasException()) {
        log.error("Exiting run with " + numPending() + " pending documents; connector threw exception", thread.getException());
        return false;
      }

      // We are done if 1) the Connector thread has terminated and therefore no more Documents will be generated,
      // 2) all published Documents and their children are accounted for (none are pending),
      // 3) there are no more Events relating to the current run to consume
      // TODO: timeouts
      if (!thread.isAlive() && !hasPending() && !manager.hasEvents()) {
        return true;
      }

      log.info("waiting on " + numPending() + " documents");
    }
  }

  @Override
  public boolean hasPending() {
    return !docIdsToTrack.isEmpty();
  }

  @Override
  public int numPending() {
    return docIdsToTrack.size();
  }

}
