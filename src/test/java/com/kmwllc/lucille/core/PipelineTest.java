package com.kmwllc.lucille.core;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class PipelineTest {

  @Test
  public void testFromConfig() throws Exception {
    String s = "pipelines = [{name:\"pipeline1\", stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}, {class:\"com.kmwllc.lucille.core.PipelineTest$Stage4\"}]}]";
    Config config = ConfigFactory.parseString(s);
    Pipeline pipeline = Pipeline.fromConfig(config, "pipeline1");
    List<Stage> stages = pipeline.getStages();
    assertEquals(stages.get(0).getClass(), Stage1.class);
    assertEquals(stages.get(1).getClass(), Stage4.class);
    assertTrue(((Stage4)stages.get(1)).isStarted());
    Document doc = new Document("d1");
    List<Document> results = pipeline.processDocument(doc);
    assertEquals("v1", doc.getString("s1"));
    assertEquals("v4", doc.getString("s4"));
  }

  @Test(expected = PipelineException.class)
  public void testFromConfigWithoutPipelinesElement() throws Exception {
    Config config = ConfigFactory.empty();
    Pipeline.fromConfig(config, "pipeline1");
  }

  @Test(expected = PipelineException.class)
  public void testFromConfigWithUnnamedPipeline() throws Exception {
    String s = "pipelines = [{stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}]}, " +
      "{name:\"pipeline1\", stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}]}]";
    Config config = ConfigFactory.parseString(s);
    Pipeline.fromConfig(config, "pipeline1");
  }

  @Test(expected = PipelineException.class)
  public void testFromConfigWithDuplicatePipelineName() throws Exception {
    String s = "pipelines = [{name:\"pipeline1\", stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}]}, " +
      "{name:\"pipeline1\", stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}]}]";
    Config config = ConfigFactory.parseString(s);
    Pipeline.fromConfig(config, "pipeline1");
  }

  @Test(expected = PipelineException.class)
  public void testFromConfigWithoutDesignatedPipeline() throws Exception {
    String s = "pipelines = [{name:\"pipeline1\", stages: [{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\"}]}]";
    Config config = ConfigFactory.parseString(s);
    Pipeline.fromConfig(config, "pipeline2");
  }

  @Test
  public void testProcessDocumentWithChildren() throws Exception {

    // create a pipeline with Stages 1 through 4 in sequence
    // d1 should flow through all stages and should get fields created by stages 1 through 4
    // d1-s2c1 and d1-s2c2 should be created by stage 2;
    //     these docs should should have fields created by downstream stages (stage 3, stage 4)
    // d1-s3c1, d1-s3c2, d1-s2c1-s3c1, d1-s2c1-s3c2, d1-s2c2-s3c1, d1-s2c2-s3c2 should be created by stage 3
    //     and should only have fields created by stage 4
    Pipeline pipeline = new Pipeline();
    Config config = ConfigFactory.empty();
    pipeline.addStage(new Stage1(config));
    pipeline.addStage(new Stage2(config));
    pipeline.addStage(new Stage3(config));
    pipeline.addStage(new Stage4(config));

    Document doc = new Document("d1");

    pipeline.startStages();
    List<Document> results = pipeline.processDocument(doc);
    pipeline.stopStages();

    ArrayList<Document> expected = new ArrayList<>();
    expected.add(Document.fromJsonString("{\"id\":\"d1\",\"s1\":\"v1\",\"s2\":\"v2\",\"s3\":\"v3\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c1\",\"s3\":\"v3\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c2\",\"s3\":\"v3\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s3c1\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s3c2\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c1-s3c1\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c1-s3c2\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c2-s3c1\",\"s4\":\"v4\"}"));
    expected.add(Document.fromJsonString("{\"id\":\"d1-s2c2-s3c2\",\"s4\":\"v4\"}"));

    assertEquals(expected.size(), results.size());
    assertTrue(results.containsAll(expected));
  }

  @Test
  public void testConditional() throws Exception {
    String s = "pipelines = [{name:\"pipeline1\", " +
        "stages: " +
        "[{class:\"com.kmwllc.lucille.core.PipelineTest$Stage1\", conditions:[{fields:[\"cond\"], values:[\"abc\",\"123\"], operator:\"must\"}]}, " +
        "{class:\"com.kmwllc.lucille.core.PipelineTest$Stage4\", conditions:[{fields:[\"cond\"], values:[\"have\",\"test\"], operator:\"must\"}]}]}]";
    Config config = ConfigFactory.parseString(s);
    Pipeline pipeline = Pipeline.fromConfig(config, "pipeline1");

    Document doc = new Document("doc");
    doc.setField("cond", "123");
    pipeline.processDocument(doc);

    assertTrue(doc.has("s1"));
    assertEquals("v1", doc.getString("s1"));
    assertFalse(doc.has("s4"));
  }

  @Test
  public void testDefaultName() throws Exception {
    Pipeline pipeline = new Pipeline();
    Config config = ConfigFactory.empty();
    pipeline.addStage(new Stage1(config));
    pipeline.addStage(new Stage2(config));
    List<Stage> stages = pipeline.getStages();
    assertEquals(2, stages.size());
    assertEquals("stage_1", stages.get(0).getName());
    assertEquals("stage_2", stages.get(1).getName());
  }

  @Test(expected = PipelineException.class)
  public void testDuplicateName() throws Exception {
    Pipeline pipeline = new Pipeline();
    Config config = ConfigFactory.empty().withValue("name", ConfigValueFactory.fromAnyRef("stage1"));
    pipeline.addStage(new Stage1(config));
    pipeline.addStage(new Stage2(config));
  }

  private static class Stage1 extends Stage {

    public Stage1(Config conf) {
      super(conf);
    }

    @Override
    public List<Document> processDocument(Document doc) throws StageException {
      doc.setField("s1", "v1");
      return null;
    }
  }

  private static class Stage2 extends Stage {

    public Stage2(Config conf) {
      super(conf);
    }

    @Override
    public List<Document> processDocument(Document doc) throws StageException {
      doc.setField("s2", "v2");
      Document c1 = new Document(doc.getId() + "-" + "s2c1");
      Document c2 = new Document(doc.getId() + "-" + "s2c2");
      ArrayList<Document> result = new ArrayList<>();
      result.add(c1);
      result.add(c2);
      return result;
    }
  }

  private static class Stage3 extends Stage {

    public Stage3(Config conf) {
      super(conf);
    }

    @Override
    public List<Document> processDocument(Document doc) {
      doc.setField("s3", "v3");
      Document c1 = new Document(doc.getId() + "-" + "s3c1");
      Document c2 = new Document(doc.getId() + "-" + "s3c2");
      ArrayList<Document> result = new ArrayList<>();
      result.add(c1);
      result.add(c2);
      return result;
    }
  }

  private static class Stage4 extends Stage {

    private boolean started = false;

    public Stage4(Config conf) {
      super(conf);
    }

    @Override
    public void start() throws StageException {
      started = true;
    }

    public boolean isStarted() {
      return started;
    }

    @Override
    public List<Document> processDocument(Document doc) throws StageException {
      doc.setField("s4", "v4");
      return null;
    }
  }
}
