package com.kmwllc.lucille.util;


import java.util.*;

public class MultiMap {

  private final Set<String> keys;
  private final Map<String, Class<?>> types;
  private final Map<String, Object> singleValued;
  private final Map<String, List<Object>> multiValued;

  private MultiMap() {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>());
  }

  // note does not deep copy
//  private MultiMap(HashMapDocument.Data other) {
//    this(new HashMap<>(other.singleValued), new HashMap<>(other.multiValued), new HashMap<>(other.types));
//  }

  private MultiMap(Map<String, Object> singleValued, Map<String, List<Object>> multiValued, Map<String, Class<?>> types) {

    if (singleValued == null || multiValued == null || types == null) {
      throw new IllegalArgumentException("constructor parameters must not be null");
    }

    if (!Collections.disjoint(singleValued.keySet(), multiValued.keySet())) {
      throw new IllegalArgumentException("singleValued and multiValued must have disjoint keys");
    }

    this.keys = new HashSet<>(singleValued.keySet());
    keys.addAll(multiValued.keySet());

    if(!keys.equals(types.keySet())) {
      throw new IllegalArgumentException("types must have the same keys as singleValued and multiValued");
    }

    this.singleValued = singleValued;
    this.multiValued = multiValued;
    this.types = types;
  }

  int size() {
    return singleValued.size() + multiValued.size();
  }

  boolean isEmpty() {
    return keys.isEmpty();
  }

  boolean containsKey(String key) {
    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }
    return keys.contains(key);
  }

  private void checkKey(String key) {
    if (containsKey(key)) {
      throw new IllegalArgumentException("key " + key + " does not exist");
    }
  }

  Object getSingle(String key) {
    checkKey(key);

    if (singleValued.containsKey(key)) {
      return singleValued.get(key);
    }

    if (!multiValued.containsKey(key)) {
      throw new IllegalStateException("key " + key + " does not exist");
    }

    // this takes care of the null handling
    List<Object> list = multiValued.get(key);
    if (list == null) {
      return null;
    }
    if (list.isEmpty()) {
      throw new IllegalArgumentException("key " + key + " is multi-valued but has no values");
    }
    return list.get(0);
  }

  List<Object> getList(String key) {
    checkKey(key);

    if (!multiValued.containsKey(key)) {
      throw new IllegalArgumentException("key " + key + " is not multi-valued");
    }
    return multiValued.get(key);
  }

  boolean isMultiValued(String key) {
    checkKey(key);
    return multiValued.containsKey(key);
  }

  void put(String key, Object value) {
    // todo can i put multiple values?

  }


    /*
    putIfAbsent ?
    remove
    addAll
    length ?
    isNonNull
     */

}
