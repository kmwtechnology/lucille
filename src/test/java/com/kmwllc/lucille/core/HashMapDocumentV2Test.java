package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

public class HashMapDocumentV2Test {

  @Test
  public void temp() throws DocumentException, JsonProcessingException {
    // todo remove

    Document document = HashMapDocumentV2.fromJsonString("{\"id\":\"id1\",\"field1\":\"value1\"}");
    Document document2 = HashMapDocumentV2.fromJsonString("{\"id\":\"id1\",\"vak\":\"2\",\"field1\":\"value1\",\"arr\":[1, 2, 3]}");



    System.out.println("hello");
  }
}
