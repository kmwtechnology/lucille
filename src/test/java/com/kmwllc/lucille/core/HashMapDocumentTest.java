package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.function.UnaryOperator;

public class HashMapDocumentTest extends DocumentTest {
  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new HashMapDocument(node);
  }

  @Override
  public Document createDocument(String id) {
    return new HashMapDocument(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new HashMapDocument(id, runId);
  }

  @Override
  public Document createDocumentFromJson(String json) throws DocumentException, JsonProcessingException {
    return HashMapDocument.fromJsonString(json);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return HashMapDocument.fromJsonString(json, idUpdater);
  }
}
