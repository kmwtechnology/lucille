package com.kmwllc.lucille.util;


import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MultiMap implements IMultiMap {

  private final static Set<Class<?>> SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
    String.class,
    Integer.class,
    Double.class,
    Long.class,
    Boolean.class,
    ObjectNode.class,
    Instant.class
  ));


  private final Set<String> keys;
  private final Map<String, Class<?>> types;
  private final Map<String, Object> singleValued;
  private final Map<String, List<Object>> multiValued;

  public MultiMap() {

    keys = new HashSet<>();
    types = new HashMap<>();
    singleValued = new HashMap<>();
    multiValued = new HashMap<>();

//    if (singleValued == null || multiValued == null || types == null) {
//      throw new IllegalArgumentException("constructor parameters must not be null");
//    }
//
//    if (!Collections.disjoint(singleValued.keySet(), multiValued.keySet())) {
//      throw new IllegalArgumentException("singleValued and multiValued must have disjoint keys");
//    }
//
//    this.keys = new HashSet<>(singleValued.keySet());
//    keys.addAll(multiValued.keySet());
//
//    if(!keys.equals(types.keySet())) {
//      throw new IllegalArgumentException("types must have the same keys as singleValued and multiValued");
//    }
//
//    this.singleValued = singleValued;
//    this.multiValued = multiValued;
//    this.types = types;
  }

  @Override
  public boolean isEmpty() {
    return keys.isEmpty();
  }

  @Override
  public boolean contains(String key) {
    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }
    return keys.contains(key);
  }

  @Override
  public boolean isMultiValued(String key) {
    checkKey(key);
    return multiValued.containsKey(key);
  }

  @Override
  public int size() {
    return singleValued.size() + multiValued.size();
  }

  @Override
  public int length(String name) {

    if (!isMultiValued(name)) {
      return 1;
    }

    List<Object> list = multiValued.get(name);
    if (list == null) {
      throw new IllegalStateException("name " + name + " is multi-valued but has no values");
    }
    return list.size();
  }

  @Override
  public Set<String> getKeys() {
    return new HashSet<>(keys);
  }

  @Override
  public Set<String> getSingleKeys() {
    return new HashSet<>(singleValued.keySet());
  }

  @Override
  public Set<String> getMultiKeys() {
    return new HashSet<>(multiValued.keySet());
  }

  @Override
  public Map<String, Object> getSingleValued() {
    return new HashMap<>(singleValued);
  }

  @Override
  public Map<String, List<Object>> getMultiValued() {
    return new HashMap<>(multiValued);
  }

  @Override
  public Class<?> getType(String key) {
    checkKey(key);
    return types.get(key);
  }

  @Override
  public Map<String, Class<?>> getTypes() {
    return new HashMap<>(types);
  }

  @Override
  public Object getOne(String name) {

    if (isMultiValued(name)) {
      List<Object> list = multiValued.get(name);
      if (list.isEmpty()) {
        throw new IllegalArgumentException("name " + name + " is multi-valued but has no values");
      }
      return list.get(0);
    }

    return singleValued.get(name);
  }

  @Override
  public List<Object> getMany(String name) {
    return isMultiValued(name) ? new ArrayList<>(multiValued.get(name)) : Collections.singletonList(singleValued.get(name));
  }

  @Override
  public void putOne(String key, Object value) throws IllegalArgumentException {

    checkAdd(key, value);

    keys.add(key);
    singleValued.put(key, value);
    multiValued.remove(key);

    if (value != null) {
      types.put(key, value.getClass());
    }
  }

  @Override
  public void putMany(String name, List<Object> values) {

    if (name == null || values == null) {
      throw new IllegalArgumentException("name and values must not be null");
    }

    // some values may be null - find non null
    Class<?> c = getClassFromList(values);
    if (c != null) {
      types.put(name, c);
    }

    keys.add(name);
    multiValued.put(name, new ArrayList<>(values));
    singleValued.remove(name);
  }


  @Override
  public void clear() {
    keys.clear();
    types.clear();
    singleValued.clear();
    multiValued.clear();
  }

  @Override
  public void rename(String oldName, String newName) {
    checkKey(oldName);

    if (newName == null) {
      throw new IllegalArgumentException("newName must not be null");
    }

    if (contains(newName)) {
      throw new IllegalArgumentException("newName must not already exist");
    }

    if (isMultiValued(oldName)) {
      multiValued.put(newName, multiValued.remove(oldName));
    } else {
      singleValued.put(newName, singleValued.remove(oldName));
    }

    keys.remove(oldName);
    keys.add(newName);
    types.put(newName, types.remove(oldName));
  }

  @Override
  public void remove(String name) {
    checkKey(name);
    keys.remove(name);
    types.remove(name);
    singleValued.remove(name);
    multiValued.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {

    if (!isMultiValued(name)) {
      throw new IllegalArgumentException("name must be multi-valued");
    }

    List<Object> list = multiValued.get(name);
    if (index < 0 || index >= list.size()) {
      throw new IllegalArgumentException("index must be in range");
    }
    list.remove(index);
  }

  @Override
  public void removeDuplicates(String name) {
    if (!isMultiValued(name)) {
      throw new IllegalArgumentException("name must be multi-valued");
    }
    multiValued.put(name, new ArrayList<>(new LinkedHashSet<>(multiValued.get(name))));
  }




//  void addValue(String key, Object value){
//
//    // todo check if want to create a list right away
//
//    keys.add(key);
//    types.put(key, value.getClass());
//
//    if (singleValued.containsKey(key)) {
//      List<Object> list = new ArrayList<>();
//      list.add(singleValued.get(key));
//      list.add(value);
//      singleValued.remove(key);
//      multiValued.put(key, list);
//    } else if (multiValued.containsKey(key)) {
//      multiValued.get(key).add(value);
//    } else {
//      multiValued.put(key, makeList(value));
//    }
//  }

//  Object getSingle(String key) {
//    checkKey(key);
//
//    if (singleValued.containsKey(key)) {
//      return singleValued.get(key);
//    }
//
//    if (!multiValued.containsKey(key)) {
//      throw new IllegalStateException("key " + key + " does not exist");
//    }
//
//    // this takes care of the null handling
//    List<Object> list = multiValued.get(key);
//    if (list == null) {
//      return null;
//    }
//    if (list.isEmpty()) {
//      throw new IllegalArgumentException("key " + key + " is multi-valued but has no values");
//    }
//    return list.get(0);
//  }
//
//  List<Object> getList(String key) {
//    checkKey(key);
//
//    if (!multiValued.containsKey(key)) {
//      throw new IllegalArgumentException("key " + key + " is not multi-valued");
//    }
//    return multiValued.get(key);
//  }

  @Override
  public String toString() {
    return Stream.concat(singleValued.entrySet().stream(), multiValued.entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MultiMap multiMap = (MultiMap) o;
    return Objects.equals(keys, multiMap.keys)
      && Objects.equals(types, multiMap.types)
      && Objects.equals(singleValued, multiMap.singleValued)
      && Objects.equals(multiValued, multiMap.multiValued);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keys, types, singleValued, multiValued);
  }

  private void checkKey(String key) {
    if (!contains(key)) {
      throw new IllegalArgumentException("key " + key + " does not exist");
    }
  }

  public static void checkAdd(String key, Object value) {

    // todo move if only one use case

    if (key == null) {
      throw new IllegalArgumentException();
    }
    if (value != null && !SUPPORTED_TYPES.contains(value.getClass())) {
      throw new IllegalArgumentException("value must be one of " + SUPPORTED_TYPES + " but was " + value.getClass());
    }
  }

  public static Class<?> getClassFromList(List<Object> list) {
    Class<?> c = null;
    for (Object o : list) {
      if (o != null) {
        if (c == null) {
          c = o.getClass();
          if (!SUPPORTED_TYPES.contains(c)) {
            throw new IllegalArgumentException("unsupported type " + c);
          }
        } else {
          if (!o.getClass().equals(c)) {
            throw new IllegalArgumentException("values must all be of the same type");
          }
        }
      }
    }
    return c;
  }

  // this is needed to be able to return null
  public static List<Object> makeList(Object value) {
    List<Object> list = new ArrayList<>();
    list.add(value);
    return list;
  }
}
