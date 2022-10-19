package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A sequence of processing Stages to be applied to incoming Documents.
 */
public class Pipeline {

  private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

  private ArrayList<Stage> stages = new ArrayList();

  public List<Stage> getStages() {
    return stages;
  }

  public void addStage(Stage stage, String metricsPrefix) throws PipelineException, StageException {
    stage.initialize(stages.size()+1, metricsPrefix);
    if (stages.stream().anyMatch(s -> stage.getName().equals(s.getName()))) {
      throw new PipelineException("Two stages cannot have the same name: " + stage.getName());
    }
    stages.add(stage);
  }

  public void addStage(Stage stage) throws PipelineException, StageException {
    addStage(stage, "default");
  }

  public void startStages() throws StageException {
    for (Stage stage : stages) {
      stage.start();
    }
  }

  public void stopStages() throws StageException {
    for (Stage stage : stages) {
      stage.stop();
    }
  }

  public void logMetrics() {
    for (Stage stage : stages) {
      stage.logMetrics();
    }
  }

  /**
   * Instantiates a Pipeline from the designated list of Stage Configs.
   * The Config for each Stage must specify the stage's class.
   */
  public static Pipeline fromConfig(List<? extends Config> stages, String metricsPrefix) throws
    Exception {
    Pipeline pipeline = new Pipeline();
    for (Config c : stages) {
      Class<?> clazz = Class.forName(c.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      Stage stage = getInstance(constructor, c);
      pipeline.addStage(stage, metricsPrefix);
    }
    pipeline.startStages();
    return pipeline;
  }

  private static Stage getInstance(Constructor<?> constructor, Config c)
    throws Exception {
    try {
      return (Stage) constructor.newInstance(c);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getTargetException();
    }
  }

  /**
   *
   * The Config is expected to have a "pipeline.<name>.stages" element
   * containing a List of stages and their settings. The list element for each Stage must specify the stage's class.
   */
  public static Pipeline fromConfig(Config config, String name, String metricsPrefix)
    throws Exception {
    if (!config.hasPath("pipelines")) {
      throw new PipelineException("No pipelines element present in config");
    }
    List<? extends Config> pipelines = config.getConfigList("pipelines");
    if (pipelines.stream().anyMatch(p -> !p.hasPath("name"))) {
      throw new PipelineException("Pipeline without name found in config");
    }
    List<Config> matchingPipelines =
      pipelines.stream().filter(p -> name.equals(p.getString("name"))).collect(Collectors.toList());
    if (matchingPipelines.size()>1) {
      throw new PipelineException("More than one pipeline found with name: " + name);
    }
    if (matchingPipelines.size()==1) {
      return fromConfig(matchingPipelines.get(0).getConfigList("stages"), metricsPrefix);
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

    String runId = document.getRunId();

    ArrayList<Document> documents = new ArrayList();
    documents.add(document);

    for (Stage stage : stages) {
      List<Document> childrenFromCurrentStage = new ArrayList();

      for (Document doc : documents) {
        List<Document> childrenOfCurrentDoc = stage.processConditional(doc);

        if (childrenOfCurrentDoc != null) {

          // if parent has a run_id, copy it to all children that don't have one
          if (runId != null) {
            for (Document child : childrenOfCurrentDoc) {
              if (!child.has(Document.RUNID_FIELD)) {
                child.initializeRunId(runId);
              }
            }
          }

          childrenFromCurrentStage.addAll(childrenOfCurrentDoc);
        }
      }

      documents.addAll(childrenFromCurrentStage);
    }

    return documents;
  }

}
