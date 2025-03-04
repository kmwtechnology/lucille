package com.kmwllc.lucille.distributed;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VerifyIngestResult {
  @Test
  public void verifyIngestResult() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String fileName = "/output/dest.json";
    int numDocs = 6;

    String output = Files.readString(Paths.get(fileName));
    JsonNode json = mapper.readTree(output);

    assertEquals(numDocs, json.get("response").get("numFound").asInt());

    List<JsonNode> docs = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      docs.add(json.get("response").get("docs").get(i));
    }

    JsonNode csvDoc1 = docs.get(0);
    assertEquals("source.csv-1", csvDoc1.get("id").asText());
    assertEquals("/conf/source.csv", csvDoc1.get("source").get(0).asText());
    assertEquals("source.csv", csvDoc1.get("filename").get(0).asText());
    assertEquals("Italy", csvDoc1.get("my_country").get(0).asText());
    assertEquals("Carbonara", csvDoc1.get("my_name").get(0).asText());
    assertEquals(30, csvDoc1.get("my_price").get(0).asInt());

    JsonNode csvDoc2 = docs.get(1);
    assertEquals("source.csv-2", csvDoc2.get("id").asText());
    assertEquals("/conf/source.csv", csvDoc2.get("source").get(0).asText());
    assertEquals("source.csv", csvDoc2.get("filename").get(0).asText());
    assertEquals("Italy", csvDoc2.get("my_country").get(0).asText());
    assertEquals("Pizza", csvDoc2.get("my_name").get(0).asText());
    assertEquals(10, csvDoc2.get("my_price").get(0).asInt());

    JsonNode csvDoc3 = docs.get(2);
    assertEquals("source.csv-3", csvDoc3.get("id").asText());
    assertEquals("/conf/source.csv", csvDoc3.get("source").get(0).asText());
    assertEquals("source.csv", csvDoc3.get("filename").get(0).asText());
    assertEquals("Korea", csvDoc3.get("my_country").get(0).asText());
    assertEquals("Tofu Soup", csvDoc3.get("my_name").get(0).asText());
    assertEquals(12, csvDoc3.get("my_price").get(0).asInt());

    JsonNode jsonDoc1 = docs.get(3);
    assertEquals("3", jsonDoc1.get("id").asText());
    assertEquals("USA", jsonDoc1.get("my_country").get(0).asText());
    assertEquals("Burger", jsonDoc1.get("my_name").get(0).asText());
    assertEquals(30, jsonDoc1.get("my_price").get(0).asInt());

    JsonNode jsonDoc2 = docs.get(4);
    assertEquals("4", jsonDoc2.get("id").asText());
    assertEquals("USA", jsonDoc2.get("my_country").get(0).asText());
    assertEquals("Salad", jsonDoc2.get("my_name").get(0).asText());
    assertEquals(10, jsonDoc2.get("my_price").get(0).asInt());

    JsonNode jsonDoc3 = docs.get(5);
    assertEquals("5", jsonDoc3.get("id").asText());
    assertEquals("France", jsonDoc3.get("my_country").get(0).asText());
    assertEquals("Wine", jsonDoc3.get("my_name").get(0).asText());
    assertEquals(12, jsonDoc3.get("my_price").get(0).asInt());
  }
}
