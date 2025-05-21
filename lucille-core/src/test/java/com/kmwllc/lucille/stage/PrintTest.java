package com.kmwllc.lucille.stage;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;
import org.mockito.MockedConstruction;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class PrintTest {

  private static final StageFactory factory = StageFactory.of(Print.class);

  @Test
  public void testBasic() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("test", "this is a test");
    doc1.initializeRunId("runID1");
    stage.processDocument(doc1);
    stage.stop();
  }

  @Test
  public void testOutputFileAppendThreadNames() throws Exception {
    Path thread1OutputPath = Paths.get("src/test/resources/PrintTest/output-thread-1.txt");
    Path thread2OutputPath = Paths.get("src/test/resources/PrintTest/output-thread-2.txt");

    // Getting the Config manually so we can override with an absolute path to the resources folder, where we will hold
    // the output files to delete later.
//    Stage stage = factory.get("PrintTest/appendThreadNames.conf");

    Document doc = Document.create("doc1");
    doc.setField("field", "value");

    Runnable runStage = () -> {
//      try {
////        stage.processDocument(doc);
//      } catch (StageException e) {
//        throw new RuntimeException(e);
//      }
    };

    Thread thread1 = new Thread(runStage, "thread-1");
    Thread thread2 = new Thread(runStage, "thread-2");

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    // check the output files are at the appropriate paths, etc.
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");
    assertEquals(Set.of("overwriteFile", "outputFile", "shouldLog", "name", "excludeFields", "conditions", "class", "conditionPolicy"),
        stage.getLegalProperties());
  }

  @Test
  public void testStop() throws Exception {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class);
        MockedConstruction<FileWriter> mockedWriter = mockConstruction(FileWriter.class)) {
      Stage stage = factory.get("PrintTest/full.conf");
      stage.stop();
      verify(mockedConstruction.constructed().get(0)).close();
    }
  }

  @Test
  public void testStopThrows() throws Exception {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class, (mock, context) -> {
      doThrow(IOException.class).when(mock).close();
    }); MockedConstruction<FileWriter> mockedWriter = mockConstruction(FileWriter.class)) {
      Stage stage = factory.get("PrintTest/full.conf");
      assertThrows(StageException.class, () -> stage.stop());
    }
  }

  @Test
  public void testAppendThrows() throws StageException {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class, (mock, context) -> {
      doThrow(IOException.class).when(mock).append(any());
    }); MockedConstruction<FileWriter> mockedWriter = mockConstruction(FileWriter.class)) {
      Stage stage = factory.get("PrintTest/full.conf");
      assertThrows(StageException.class, () -> stage.processDocument(Document.create("foo")));
    }
  }
}
