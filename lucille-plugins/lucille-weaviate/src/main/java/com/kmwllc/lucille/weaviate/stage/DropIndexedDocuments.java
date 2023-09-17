package com.kmwllc.lucille.weaviate.stage;

import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.StageException;

import io.weaviate.client.base.Result;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.v1.graphql.query.Get;
import io.weaviate.client.v1.graphql.query.fields.Field;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import io.weaviate.client.v1.auth.exception.AuthException;


public class DropIndexedDocuments extends Stage {

  private static final Logger log = LoggerFactory.getLogger(DropIndexedDocuments.class);
  private final static Gson GSON = new Gson();

  private final String weaviateClassName;
  private final String idDestinationName;
  private final String fileWithIds;

  private final int batchSize;

  private final Set<String> ids;
  private final WeaviateClient client;

  public DropIndexedDocuments(Config config) throws StageException {

    super(config, new StageSpec()
        .withRequiredProperties(
            "weaviate.host",
            "weaviate.apiKey",
            "fileWithIds"
        ).withOptionalProperties(
            "weaviate.className",
            "weaviate.idDestinationName",
            "batchSize"
        ));

    // optional
    weaviateClassName = ConfigUtils.getOrDefault(config, "weaviate.className", "Document");
    idDestinationName = ConfigUtils.getOrDefault(config, "weaviate.idDestinationName", "id_original");
    batchSize = ConfigUtils.getOrDefault(config, "batchSize", 1000);

    // required
    fileWithIds = config.getString("fileWithIds");
    io.weaviate.client.Config weaviateConfig =
        new io.weaviate.client.Config("https", config.getString("weaviate.host"), null, 6000, 6000, 6000);

    try {
      this.client = WeaviateAuthClient.apiKey(weaviateConfig, config.getString("weaviate.apiKey"));
    } catch (AuthException e) {
      throw new RuntimeException("Couldn't connect to Weaviate instance", e);
    }

    // if given path to file with IDs, use that, otherwise get IDs from Weaviate and save them to a file with that path
    ids = new File(fileWithIds).exists() ? getIdsFromFile() : getIdsFromWeaviate();
    log.info("Got {} IDs", ids.size());
  }

  private Set<String> getIdsFromFile() {
    try (BufferedReader reader = new BufferedReader(new FileReader(fileWithIds))) {
      Set<String> ids = new HashSet<>();
      String line;
      while ((line = reader.readLine()) != null) {
        ids.add(line);
      }
      return ids;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<String> getIdsFromWeaviate() throws StageException {

    int numIndexed = getCount();  // could be long but no need right now
    log.info("Getting IDs of {} indexed documents", numIndexed);

    int count = 0;
    int totalBatches = numIndexed / batchSize;

    String lastID = null;

    Set<String> ids = new HashSet<>(numIndexed);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileWithIds, false))) {

      while (numIndexed > 0) {

        // get a batch of original ids and the last id in the batch
        Pair<List<String>, String> batch = getBatchIds(lastID);

        // extract batch original ids and last id from pair
        List<String> batchIds = batch.getFirst();
        lastID = batch.getSecond();

        // add ids to set
        ids.addAll(batchIds);

        // make a string of all the ids and write them to a file
        StringBuilder sb = new StringBuilder();
        for (String id : batchIds) {
          sb.append(id).append("\n");
        }
        writer.write(sb.toString());
        writer.flush();

        // update counter and number left to index
        count++;
        numIndexed -= batchSize;

        log.info("Processed batch {} of {}", count, totalBatches);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    log.info("Wrote IDs to " + fileWithIds);
    return ids;
  }

  private Pair<List<String>, String> getBatchIds(String lastID) throws StageException {

    // initialize fields to get from Weaviate
    Field[] fields = new Field[]{
        Field.builder()
            .name(idDestinationName)
            .build(),
        Field.builder()
            .name("_additional")
            .fields(
                Field.builder()
                    .name("id")
                    .build()
            ).build()
    };

    // initialize a request
    Get query = client
        .graphQL()
        .get()
        .withClassName(weaviateClassName)
        .withFields(fields)
        .withLimit(batchSize);

    // if lastID is not null, add it to the request for pagination
    if (lastID != null) {
      query.withAfter(lastID);
    }

    // run query
    Result<GraphQLResponse> response = query.run();

    if (response.hasErrors()) {
      throw new StageException("Error getting ids from Weaviate: " + response.getError().toString());
    }

    JsonArray array = GSON.toJsonTree(response.getResult().getData())
        .getAsJsonObject()
        .get("Get")
        .getAsJsonObject()
        .get(weaviateClassName)
        .getAsJsonArray();

    List<String> parsedIDs = new ArrayList<>();
    for (int i = 0; i < array.size(); i++) {
      JsonObject obj = array.get(i).getAsJsonObject();
      JsonObject additional = obj.get("_additional").getAsJsonObject();

      parsedIDs.add(obj.get("id_original").getAsString());

      if (i == array.size() - 1) {
        lastID = additional.get("id").getAsString();
      }
    }

    return new Pair<>(parsedIDs, lastID);
  }

  private int getCount() throws StageException {

    Field fields = Field.builder()
        .name("meta")
        .fields(
            Field.builder()
                .name("count")
                .build()
        ).build();

    Result<GraphQLResponse> response = client
        .graphQL()
        .aggregate()
        .withClassName(weaviateClassName)
        .withFields(fields)
        .run();

    if (response.hasErrors()) {
      throw new StageException("Error getting count from Weaviate: " + response.getError().toString());
    }

    JsonArray array = GSON.toJsonTree(response.getResult().getData())
        .getAsJsonObject()
        .get("Aggregate")
        .getAsJsonObject()
        .get(weaviateClassName)
        .getAsJsonArray();

    if (array.size() != 1) {
      throw new StageException("Expected 1 result from Weaviate, got " + array.size());
    }

    return array
        .get(0)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("count")
        .getAsInt();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) {
    String id = doc.getId();

    if (ids.contains(id)) {

      // set drop document to true
      doc.setDropped(true);

      // remove from set
      ids.remove(id);
    }
    return null;
  }

  public static class Pair<A, B> {

    private final A a;
    private final B b;

    public Pair(A a, B b) {
      this.a = a;
      this.b = b;
    }

    public A getFirst() {
      return a;
    }

    public B getSecond() {
      return b;
    }
  }
}
