package com.kmwllc.lucille.util;


import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMultiMap {

  boolean isEmpty();

  boolean contains(String key);

  boolean isMultiValued(String name);

  int size();

  int length(String name);

  Set<String> getKeys();

  Set<String> getSingleKeys();

  Set<String> getMultiKeys();

  Map<String, Object> getSingleValued();

  Map<String, List<Object>> getMultiValued();

  Class<?> getType(String key);

  Map<String, Class<?>> getTypes();

  Object getOne(String name);

  List<Object> getMany(String name);

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

  // todo addAll / addOne
}
