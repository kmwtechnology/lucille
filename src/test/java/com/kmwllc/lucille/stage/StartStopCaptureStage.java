package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

public class StartStopCaptureStage extends AbstractStage {

  public static boolean startCalled = false;
  public static boolean stopCalled = false;

  public StartStopCaptureStage(Config config) {
    super(config);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
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
