package com.kmwllc.lucille.mongodb.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.function.UnaryOperator;

import org.bson.Document;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * A simple mongodb connector that can iterate the documents in 
 * collection in a mongodb instance.
 */
public class MongoDBConnector extends AbstractConnector {

  private final String uri;
  private final String database;
  private final String collection;

  public MongoDBConnector(Config config) {
    super(config);
    this.uri = config.hasPath("uri") ? config.getString("uri") : "mongodb://localhost:27017";
    this.database = config.hasPath("database") ? config.getString("database") : "db";
    this.collection = config.hasPath("collection") ? config.getString("collection") : "collection";
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    // Replace the placeholder with your MongoDB deployment's connection string
    String connectionString = uri;
    try (MongoClient mongoClient = MongoClients.create(connectionString)) {
      MongoDatabase db = mongoClient.getDatabase(database);
      MongoCollection<Document> col = db.getCollection(collection);
      FindIterable<Document> iterDoc = col.find();
      Iterator<Document> it = iterDoc.iterator();
      int i = 0; 
      while (it.hasNext()) {
        Document currDoc = it.next();
        // let's get this current doc as a json string.. 
        String mongoJsonDoc = currDoc.toJson();
        String id = currDoc.get("_id").toString();
        ObjectMapper mapper = new ObjectMapper();
        try {
          JsonNode json = mapper.readTree( mongoJsonDoc );
          ObjectNode node = (ObjectNode) json;
          node.remove("_id");
          node.put("id", id); 
          String updated = mapper.writeValueAsString(node);
          JsonDocument lDoc = JsonDocument.fromJsonString(updated);
          publisher.publish(lDoc);
        } catch (JsonMappingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (JsonProcessingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (DocumentException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      }
    }
  }

}
