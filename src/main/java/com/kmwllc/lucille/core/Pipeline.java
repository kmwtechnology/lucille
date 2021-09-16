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

  /**
   * Instantiates a Pipeline from the designated list of Stage Configs.
   * The Config for each Stage must specify the stage's class.
   */
  public static Pipeline fromConfig(List<? extends Config> stages) throws ClassNotFoundException, NoSuchMethodException,
    IllegalAccessException, InvocationTargetException, InstantiationException, StageException {
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

  /**
   *
   * The Config is expected to have a "pipeline.<name>.stages" element
   * containing a List of stages and their settings. The list element for each Stage must specify the stage's class.
   */
  public static Pipeline fromConfig(Config config, String name) throws ClassNotFoundException, NoSuchMethodException,
    IllegalAccessException, InvocationTargetException, InstantiationException, StageException, PipelineException {
    if (!config.hasPath("pipelines")) {
      throw new PipelineException("No pipelines element present in config");
    }
    List<? extends Config> pipelines = config.getConfigList("pipelines");
    for (Config pipeline : pipelines) {
      if (!pipeline.hasPath("name")) {
        throw new PipelineException("Pipeline without name found in config");
      }
      if (name.equals(pipeline.getString("name"))) {
        return fromConfig(pipeline.getConfigList("stages"));
      }
    }
    throw new PipelineException("No pipeline found with name: " + name);
  }

  public static Integer getIntProperty(Config config, String pipelineName, String propertyName) throws PipelineException {
    if (!config.hasPath("pipelines")) {
      throw new PipelineException("No pipelines element present in config");
    }
    List<? extends Config> pipelines = config.getConfigList("pipelines");
    for (Config pipeline : pipelines) {
      if (!pipeline.hasPath("name")) {
        continue;
      }
      if (pipelineName.equals(pipeline.getString("name"))) {
        if (pipeline.hasPath(propertyName)) {
          return pipeline.getInt(propertyName);
        }
        return null;
      }
    }
    return null;
  }

  /**
   * Passes a Document through the designated sequence of stages and returns a list containing
   * the input Document as the first element, along with any child documents generated.
   *
   * Child documents are passed through downstream stages only. For example, if we have a sequence of stages
   * S1, S2, S3, and if S2 generates a child document, the child document will be passed through S3 only.
   *
   */
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

  /**
   * Creates a Document from a given Kafka ConsumerRecord and delegates to processDocument.
   */
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
