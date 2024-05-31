package com.kmwllc.lucille.stage;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import org.mockito.MockedConstruction;
import java.io.BufferedWriter;
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
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");
    assertEquals(Set.of("overwriteFile", "outputFile", "shouldLog", "name", "excludeFields", "conditions", "class"),
        stage.getLegalProperties());
  }

  @Test
  public void testStop() throws Exception {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class)) {
      Stage stage = factory.get("PrintTest/full.conf");
      stage.stop();
      verify(mockedConstruction.constructed().get(0)).close();
    }
  }

  @Test
  public void testStopThrows() throws Exception {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class, (mock, context) -> {
      doThrow(IOException.class).when(mock).close();
    })) {
      Stage stage = factory.get("PrintTest/full.conf");
      assertThrows(StageException.class, () -> stage.stop());
    }
  }

  @Test
  public void testAppendThrows() throws StageException {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class, (mock, context) -> {
      doThrow(IOException.class).when(mock).append(any());
    })) {
      Stage stage = factory.get("PrintTest/full.conf");
      assertThrows(StageException.class, () -> stage.processDocument(Document.create("foo")));
    }
  }
}
