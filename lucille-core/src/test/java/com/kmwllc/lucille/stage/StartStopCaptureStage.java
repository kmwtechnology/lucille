package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;

public class StartStopCaptureStage extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().build();

  public static boolean startCalled = false;
  public static boolean stopCalled = false;

  public StartStopCaptureStage(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    return null;
  }

  @Override
  public void start() throws StageException {
    if (startCalled) {
      throw new StageException("Start already called");
    }
    startCalled = true;
  }

  @Override
  public void stop() throws StageException {
    if (stopCalled) {
      throw new StageException("Stop already called");
    }
    stopCalled = true;
  }

  public static void reset() {
    startCalled = false;
    stopCalled = false;
  }


}
