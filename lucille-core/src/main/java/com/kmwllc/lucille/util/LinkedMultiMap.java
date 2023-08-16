package com.kmwllc.lucille.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kmwllc.lucille.core.HashMapDocument;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonIgnoreProperties(value = { "keys", "empty" })
public class LinkedMultiMap implements MultiMap, Serializable {

  static final long serialVersionUID = 1L;

  private LinkedHashMap<String, Object> data;

  public static final Set<Class<?>> SUPPORTED_TYPES =
    Collections.unmodifiableSet(new HashSet<>(
      List.of(
        String.class,
        Integer.class,
        Double.class,
        Float.class,
        Long.class,
        Boolean.class,
        ObjectNode.class,
        Instant.class,
        HashMapDocument.class,
        TextNode.class,
        ArrayNode.class,
        byte[].class)));


  public LinkedMultiMap() {
    this.data = new LinkedHashMap<>();
  }

  public LinkedMultiMap(LinkedHashMap data) {
    this.data = data;
  }

  public LinkedHashMap getData() {
    return data;
  }

  public void setData(LinkedHashMap data) {
    this.data = data;
  }

  @Override
  public boolean isEmpty() {
    return data.isEmpty();
  }

  @Override
  public boolean contains(String key) {
    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }
    return data.containsKey(key);
  }

  @Override
  public boolean isMultiValued(String key) {
    checkKey(key);
    return data.get(key) instanceof List;
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public int length(String name) {

    if (!isMultiValued(name)) {
      return 1;
    }

    List list = (List)data.get(name);

    // TODO remove?
    if (list == null) {
      throw new IllegalStateException("name " + name + " is multi-valued but has no values");
    }

    return list.size();
  }

  @Override
  public Set<String> getKeys() {
    return data.keySet();
  }

  @Override
  public Object getOne(String name) {

    if (isMultiValued(name)) {
      List<Object> list = (List)data.get(name);
      if (list.isEmpty()) {
        throw new IllegalArgumentException("name " + name + " is multi-valued but has no values");
      }
      return list.get(0);
    }

    return data.get(name);
  }

  @Override
  public List getMany(String name) {
    return isMultiValued(name)
        ? (List)data.get(name)
        : Collections.singletonList(data.get(name));
  }

  @Override
  public MultiMap deepCopy() {
    return new LinkedMultiMap((LinkedHashMap)data.clone());
  }

  @Override
  public void putOne(String key, Object value) throws IllegalArgumentException {

    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }

    if (value != null) {
      Class<?> type = value.getClass();
      if (!SUPPORTED_TYPES.contains(type)) {
        throw new IllegalArgumentException(
            "value must be one of " + SUPPORTED_TYPES + " but was " + type);
      }
    }

    data.put(key, value);
  }

  @Override
  public void putMany(String name, List<Object> values) {

    if (name == null || values == null) {
      throw new IllegalArgumentException("name and values must not be null");
    }

    // some values may be null - find non null
    getClassFromList(values);

    data.put(name, values);
  }

  @Override
  public void add(String name, Object value) {

    if (!contains(name)) {
      data.put(name, makeList(value));
      return;
    }

    if (isMultiValued(name)) {
      ((List)data.get(name)).add(value);
    } else {
      data.put(name, makeList(data.remove(name), value));
    }
  }

  @Override
  public void addAll(String name, List<Object> values) {
    for (Object value : values) {
      add(name, value);
    }
  }

  @Override
  public void setOrAdd(
      String name,
      Object value) { // todo if used to be an empty list need to add type, write a test
    if (!contains(name)) {
      putOne(name, value);
    } else {
      add(name, value);
    }
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void rename(String oldName, String newName) {
    checkKey(oldName);
    if (newName == null || contains(newName)) {
      throw new IllegalArgumentException("newName must not be null or already exist");
    }

    if (isMultiValued(oldName)) {
      putMany(newName, (List)data.remove(oldName));
    } else {
      putOne(newName, data.remove(oldName));
    }
  }

  @Override
  public void remove(String name) {
    checkKey(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    if (!isMultiValued(name)) {
      throw new IllegalArgumentException("name must be multi-valued");
    }
    List<Object> list = (List)data.get(name);
    if (index < 0 || index >= list.size()) {
      throw new IllegalArgumentException(
          "given index " + index + " is out of bounds for list of size " + list.size());
    }
    list.remove(index);
  }

  @Override
  public void removeDuplicates(String name) {
    if (!isMultiValued(name)) {
      throw new IllegalArgumentException("name must be multi-valued");
    }
    data.put(name, new ArrayList(new LinkedHashSet((List)data.get(name))));
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LinkedMultiMap multiMap = (LinkedMultiMap) o;
    return Objects.equals(data, multiMap.data);
  }

  @Override
  public int hashCode() {
    return data.hashCode();
  }

  private void checkKey(String key) {
    if (!contains(key)) {
      throw new IllegalArgumentException("key " + key + " does not exist");
    }
  }

  private Class<?> getClassFromList(List<Object> list) {
    Class<?> type = null;
    for (Object o : list) {
      if (o != null) {
        if (type == null) {
          type = o.getClass();
          if (!SUPPORTED_TYPES.contains(type)) {
            throw new IllegalArgumentException("unsupported type " + type);
          }
        } else {
          if (!o.getClass().equals(type)) {
            throw new IllegalArgumentException("values must all be of the same type");
          }
        }
      }
    }
    return type;
  }

  // todo check if can be removed
  private List<Object> makeList(Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }
}
