package com.kmwllc.lucille.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a map with string keys and single or multi-valued object values. Offers functionality similar to {@link Map} but
 * differentiates between setting and retrieving a single or collection of values.
 */
public interface MultiMap {

  boolean isEmpty();

  boolean contains(String key);

  boolean isMultiValued(String name);

  int size();

  int length(String name);

  Set<String> getKeys();

  Object getOne(String name);

  List<Object> getMany(String name);

  MultiMap deepCopy();

  void putOne(String name, Object value);

  void putMany(String name, List<Object> values);

  void add(String name, Object value);

  void addAll(String name, List<Object> values);

  void setOrAdd(String name, Object value);

  void clear();

  void rename(String oldName, String newName);

  void remove(String name);

  void removeFromArray(String name, int index);

  void removeDuplicates(String name);
}
