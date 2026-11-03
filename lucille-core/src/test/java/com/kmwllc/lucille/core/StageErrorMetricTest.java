package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.ConfigFactory;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

public class StageErrorMetricTest {

  @Test
  public void testStageExceptionIsCountedAndWrapped() throws Exception {
    FailingStage stage = stage("stageException");
    StageException cause = new StageException("cannot process");
    stage.failure = cause;

    Iterator<Document> results = stage.apply(List.of(Document.create("doc1")).iterator());
    RuntimeException e = assertThrows(RuntimeException.class, results::next);
    assertSame(cause, e.getCause());
    // the FAIL event carries the stage's message, not the wrapper's
    assertEquals("cannot process", Event.failureMessage(e));
    assertEquals(1, errors("stageException"));
  }

  @Test
  public void testRuntimeExceptionIsCounted() throws Exception {
    FailingStage stage = stage("runtimeException");
    IllegalArgumentException cause = new IllegalArgumentException("bad value");
    stage.failure = cause;

    Iterator<Document> results = stage.apply(List.of(Document.create("doc1")).iterator());
    assertSame(cause, assertThrows(IllegalArgumentException.class, results::next));
    assertEquals(1, errors("runtimeException"));
  }

  @Test
  public void testFailureWhileProducingAChildIsCounted() throws Exception {
    FailingStage stage = stage("childFailure");
    stage.failChildren = true;

    Iterator<Document> results = stage.apply(List.of(Document.create("doc1")).iterator());
    assertThrows(IllegalStateException.class, results::next);
    assertEquals(1, errors("childFailure"));
  }

  @Test
  public void testFailureInAnUpstreamStageIsNotCountedDownstream() throws Exception {
    FailingStage upstream = stage("upstream");
    upstream.failure = new IllegalArgumentException("bad value");
    FailingStage downstream = stage("downstream");

    Iterator<Document> results = downstream.apply(upstream.apply(List.of(Document.create("doc1")).iterator()));
    assertThrows(IllegalArgumentException.class, results::next);
    assertEquals(1, errors("upstream"));
    assertEquals(0, errors("downstream"));
  }

  private static FailingStage stage(String name) throws StageException {
    FailingStage stage = new FailingStage(name);
    stage.initialize(0, prefix(name));
    return stage;
  }

  // a prefix per test, so counts from other tests in the shared registry do not leak in
  private static String prefix(String name) {
    return "StageErrorMetricTest." + name;
  }

  private static long errors(String name) {
    return SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG)
        .counter(prefix(name) + ".stage." + name + ".errors").getCount();
  }

  public static class FailingStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    Exception failure;
    boolean failChildren;

    public FailingStage(String name) {
      super(ConfigFactory.parseString("name: " + name));
    }

    @Override
    public Iterator<Document> processDocument(Document doc) throws StageException {
      if (failure instanceof StageException stageException) {
        throw stageException;
      }
      if (failure instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (failChildren) {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Document next() {
            throw new IllegalStateException("could not produce child");
          }
        };
      }
      return null;
    }
  }
}
