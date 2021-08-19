package com.kmwllc.lucille.callback;

public interface RunnerMessageManager {
  Event pollEvent() throws Exception;

  boolean allEventsConsumed(String runId) throws Exception;

  void close() throws Exception;
}
