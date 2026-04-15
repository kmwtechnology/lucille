package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
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
  private static final String outputFilePath = "src/test/resources/PrintTest/output.txt";

  @Test
  public void testBasic() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");

    Document doc1 = Document.create("doc1", "runID1");
    doc1.setField("test", "this is a test");
    stage.processDocument(doc1);
    stage.stop();
  }

  @Test
  public void testOutputFileAppendThreadNames() throws Exception {
    String baseOutputFileAbsolute = Paths.get("src/test/resources/PrintTest/output-.txt").toAbsolutePath().toString();
    Path thread1OutputPath = Paths.get("src/test/resources/PrintTest/output-thread-1.txt");
    Path thread2OutputPath = Paths.get("src/test/resources/PrintTest/output-thread-2.txt");

    // Getting the Config manually so we can override with an absolute path to the resources folder, where we will hold
    // the output files to delete later.
    Config appendThreadConfig = ConfigFactory.parseResourcesAnySyntax("PrintTest/appendThreadNames.conf")
        .withValue("outputFile", ConfigValueFactory.fromAnyRef(baseOutputFileAbsolute));

    Document doc = Document.create("doc1");
    doc.setField("field", "value");

    // NOTE: The Stage is *purposely* constructed and started in the main thread. This mimics how pipelines work in a
    // multithreaded run. Have to make sure the threads still work / handle this... write to the correct file.
    Stage stage1 = factory.get(appendThreadConfig);
    Stage stage2 = factory.get(appendThreadConfig);

    Thread thread1 = new Thread(() -> {
      try {
        stage1.processDocument(doc);
      } catch (StageException e) {
        throw new RuntimeException(e);
      }
    }, "thread-1");

    Thread thread2 = new Thread(() -> {
      try {
        stage2.processDocument(doc);
      } catch (StageException e) {
        throw new RuntimeException(e);
      }
    }, "thread-2");

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    stage1.stop();
    stage2.stop();

    // check the output files are at the appropriate paths, appropriate content, etc.
    try (
        InputStream thread1Stream = new FileInputStream(thread1OutputPath.toFile());
        InputStream thread2Stream = new FileInputStream(thread2OutputPath.toFile())
    ) {
      String thread1Contents = new String(thread1Stream.readAllBytes());
      String thread2Contents = new String(thread2Stream.readAllBytes());

      assertTrue(thread1Contents.startsWith("{\"id\":\"doc1\",\"field\":\"value\"}"));
      assertTrue(thread2Contents.startsWith("{\"id\":\"doc1\",\"field\":\"value\"}"));
    }

    // Delete the files
    Files.delete(thread1OutputPath);
    Files.delete(thread2OutputPath);
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");
    assertEquals(Set.of("overwriteFile", "outputFile", "shouldLog", "name", "blacklist", "whitelist", "conditions", "class",
            "conditionPolicy", "appendThreadName"),
        stage.getLegalProperties());
  }

  @Test
  public void testStop() throws Exception {
    try (MockedConstruction<BufferedWriter> mockedConstruction = mockConstruction(BufferedWriter.class);
        MockedConstruction<FileWriter> mockedWriter = mockConstruction(FileWriter.class)) {
      Stage stage = factory.get("PrintTest/full.conf");

      // have to process something so the ThreadLocal FileWriter actually gets built.
      Document doc = Document.create("test");
      stage.processDocument(doc);

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

      // have to process something so the ThreadLocal FileWriter actually gets built.
      Document doc = Document.create("test");
      stage.processDocument(doc);

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

  @Test
  public void testBadOutputPath() throws StageException {
    Stage stage = factory.get("PrintTest/badOutputPath.conf");

    // error for bad file path (I/O) is not thrown until a document actually gets processed
    Document doc = Document.create("doc1");
    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  @Test
  public void testOverwriting() throws Exception {
    Stage stage = factory.get("PrintTest/outputFileOverwrite.conf");

    Document doc = Document.create("doc", "run123");
    doc.setField("field", "value");

    stage.processDocument(doc);
    stage.stop();

    // should be the doc written, as usual.
    String contents = new String(Files.readAllBytes(Paths.get(outputFilePath)));
    assertTrue(contents.startsWith("{\"id\":\"doc\",\"field\":\"value\"}"));

    // now, we will attempt to overwrite the file contents
    stage = factory.get("PrintTest/outputFileOverwrite.conf");

    doc = Document.create("doc", "run456");
    doc.setField("abc", "123");
    // in blacklist, shouldn't be part of the response.
    doc.setField("to_remove", "abcdef");

    stage.processDocument(doc);
    stage.stop();

    // should be only the new contents - overwrite the old contents
    contents = new String(Files.readAllBytes(Paths.get(outputFilePath)));
    assertTrue(contents.startsWith("{\"id\":\"doc\",\"abc\":\"123\"}"));

    Files.delete(Paths.get(outputFilePath));
  }

  @Test
  public void testWhitelistAndBlacklist() throws Exception {
    Stage stage = factory.get("PrintTest/whitelistAndBlacklist.conf");

    Document doc = Document.create("doc", "run");
    doc.setField("field", "value");
    doc.setField("removeField1", "value");
    doc.setField("removeField2", "value");
    doc.setField("keepField1", "value");

    stage.processDocument(doc);
    stage.stop();

    // only id (required for a valid Document) and keepField1 (in whitelist, not in blacklist) should be included
    String contents = new String(Files.readAllBytes(Paths.get(outputFilePath)));
    assertTrue(contents.startsWith("{\"id\":\"doc\",\"keepField1\":\"value\"}"));
  }

  @Test
  public void testRunIdPreservedWhenWhitelisted() throws Exception {
    Stage stage = factory.get("PrintTest/whitelistWithRunId.conf");

    Document doc = Document.create("doc", "myRunId");
    doc.setField("keepField", "value");
    doc.setField("dropField", "value");

    stage.processDocument(doc);
    stage.stop();

    String contents = new String(Files.readAllBytes(Paths.get(outputFilePath)));
    assertTrue(contents.contains("\"run_id\":\"myRunId\""));
    assertTrue(contents.contains("\"keepField\":\"value\""));
    assertFalse(contents.contains("dropField"));
    Files.delete(Paths.get(outputFilePath));
  }

  @Test
  public void testOriginalDocUnchangedAfterFilter() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");

    Document doc = Document.create("doc", "run1");
    doc.setField("test", "value");
    doc.setField("otherField", "value");

    stage.processDocument(doc);

    assertEquals("run1", doc.getRunId());
    assertTrue(doc.has("otherField"));
  }
}
