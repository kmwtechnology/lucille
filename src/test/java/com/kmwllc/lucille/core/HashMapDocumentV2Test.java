package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.function.UnaryOperator;


public class HashMapDocumentV2Test extends DocumentTest {

  @Override
  public Document createDocument(String id) {
    return new HashMapDocumentV2(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new HashMapDocumentV2(id, runId);
  }

  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new HashMapDocumentV2(node);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return HashMapDocumentV2.fromJsonString(json, idUpdater);
  }
}
