package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * A sequence of processing Stages to be applied to incoming Documents.
 */
public class Pipeline {

  private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

  private ArrayList<Stage> stages = new ArrayList();

  public List<Stage> getStages() {
    return stages;
  }

  public void addStage(Stage stage) {
    stages.add(stage);
  }

  public void startStages() throws StageException {
    for (Stage stage : stages) {
      stage.start();
    }
  }

  public static Pipeline fromConfig(Config config) throws ClassNotFoundException, NoSuchMethodException,
    IllegalAccessException, InvocationTargetException, InstantiationException, StageException {
    List<? extends Config> stages = config.getConfigList("stages");
    Pipeline pipeline = new Pipeline();
    for (Config c : stages) {
      Class<?> clazz = Class.forName(c.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      Stage stage = (Stage) constructor.newInstance(c);
      pipeline.addStage(stage);
    }
    pipeline.startStages();
    return pipeline;
  }

  public List<Document> processDocument(Document document) throws StageException {
    ArrayList<Document> documents = new ArrayList();
    documents.add(document);

    for (Stage stage : stages) {
      List<Document> childrenFromCurrentStage = new ArrayList();

      for (Document doc : documents) {
        List<Document> childrenOfCurrentDoc = stage.processDocument(doc);
        if (childrenOfCurrentDoc != null) {
          childrenFromCurrentStage.addAll(childrenOfCurrentDoc);
        }
      }

      documents.addAll(childrenFromCurrentStage);
    }

    return documents;
  }

  public List<Document> processKafkaJsonMessage(ConsumerRecord<String,String> record) throws Exception {
    Document document = null;

    try {
      document = Document.fromJsonString(record.value());
      log.info("Processing document " + document.getId());

      if (!document.getId().equals(record.key())) {
        log.warn("Kafka message key " + record.key() + " does not match document ID " + document.getId());
      }

      return processDocument(document);
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      log.error("Processing error", t);
    }

    return null;
  }

}
