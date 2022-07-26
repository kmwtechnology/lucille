package com.kmwllc.lucille.core;

public interface RetryCounter {
  boolean add(JsonDocument document);

  void remove(JsonDocument document);
}
