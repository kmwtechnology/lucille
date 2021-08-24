package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;

public interface RunnerMessageManager {
  Event pollEvent() throws Exception;

  boolean hasEvents(String runId) throws Exception;

  void close() throws Exception;
}
