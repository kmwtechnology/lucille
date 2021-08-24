package com.kmwllc.lucille.callback;

public interface RunnerMessageManager {
  Event pollEvent() throws Exception;

  boolean hasEvents(String runId) throws Exception;

  void close() throws Exception;
}
