package com.kmwllc.lucille.message;

public class MessageManagerFactory {

  private static final MessageManagerFactory INSTANCE = new MessageManagerFactory();

  private static boolean localMode = false;

  private MessageManagerFactory() {
  }

  public static MessageManagerFactory getInstance() {
    return INSTANCE;
  }

  public synchronized void setLocalMode() {
    localMode = true;
  }

  public boolean isLocalMode() {
    return localMode;
  }

  public PublisherMessageManager getPublisherMessageManager() {
    return localMode ? PersistingLocalMessageManager.getInstance() : new KafkaPublisherMessageManager();
  }

  public IndexerMessageManager getIndexerMessageManager() {
    return localMode ? PersistingLocalMessageManager.getInstance() : new KafkaIndexerMessageManager();
  }

  public RunnerMessageManager getRunnerMessageManager(String runId) {
    return localMode ? PersistingLocalMessageManager.getInstance() : new KafkaRunnerMessageManager(runId);
  }

  public WorkerMessageManager getWorkerMessageManager() {
    return localMode ? PersistingLocalMessageManager.getInstance() : new KafkaWorkerMessageManager();
  }

}
