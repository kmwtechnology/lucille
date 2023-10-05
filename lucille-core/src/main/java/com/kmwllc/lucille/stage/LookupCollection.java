package com.kmwllc.lucille.stage;

public interface LookupCollection<T> {

  boolean add(T value);

  boolean contains(T value);

  boolean remove(T value);
}
