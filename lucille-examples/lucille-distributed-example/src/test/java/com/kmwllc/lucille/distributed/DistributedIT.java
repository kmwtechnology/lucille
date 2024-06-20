package com.kmwllc.lucille.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DistributedIT {

  @Test
  public void distributedIT() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String fileName = "runner/output/dest.json";
    int numDocs = 6;
    try {
      String output = Files.readString(Paths.get(fileName));
      JsonNode json = mapper.readTree(output);

      assertEquals(numDocs, json.get("response").get("numFound").asInt());

      List<JsonNode> docs = new ArrayList<>();
      for (int i = 0; i < numDocs; i++) {
        docs.add(json.get("response").get("docs").get(i));
      }

      assertTrue(docs.stream()
          .anyMatch(doc -> doc.get("id").asText().equals("source.csv-1")
              && doc.get("source").get(0).asText().equals("conf/source.csv")
              && doc.get("filename").get(0).asText().equals("source.csv") && doc.get("my_country").get(0).asText().equals("Italy")
              && doc.get("my_price").get(0).asInt() == 30 && doc.get("my_name").get(0).asText().equals("Carbonara")));
      assertTrue(docs.stream()
          .anyMatch(doc -> doc.get("id").asText().equals("source.csv-2")
              && doc.get("source").get(0).asText().equals("conf/source.csv")
              && doc.get("filename").get(0).asText().equals("source.csv") && doc.get("my_country").get(0).asText().equals("Italy")
              && doc.get("my_price").get(0).asInt() == 10 && doc.get("my_name").get(0).asText().equals("Pizza")));
      assertTrue(docs.stream()
          .anyMatch(doc -> doc.get("id").asText().equals("source.csv-3")
              && doc.get("source").get(0).asText().equals("conf/source.csv")
              && doc.get("filename").get(0).asText().equals("source.csv") && doc.get("my_country").get(0).asText().equals("Korea")
              && doc.get("my_price").get(0).asInt() == 12 && doc.get("my_name").get(0).asText().equals("Tofu Soup")));

      assertTrue(
          docs.stream().anyMatch(doc -> doc.get("id").asText().equals("3") && doc.get("my_country").get(0).asText().equals("USA")
              && doc.get("my_price").get(0).asInt() == 30 && doc.get("my_name").get(0).asText().equals("Burger")));
      assertTrue(
          docs.stream().anyMatch(doc -> doc.get("id").asText().equals("4") && doc.get("my_country").get(0).asText().equals("USA")
              && doc.get("my_price").get(0).asInt() == 10 && doc.get("my_name").get(0).asText().equals("Salad")));
      assertTrue(
          docs.stream().anyMatch(doc -> doc.get("id").asText().equals("5") && doc.get("my_country").get(0).asText().equals("France")
              && doc.get("my_price").get(0).asInt() == 12 && doc.get("my_name").get(0).asText().equals("Wine")));
    } finally {
      new File(fileName).delete();
    }
  }
}
