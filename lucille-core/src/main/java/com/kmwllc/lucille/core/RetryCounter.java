package com.kmwllc.lucille.core;

public interface RetryCounter {

  boolean add(Document document);

  void remove(Document document);
}
