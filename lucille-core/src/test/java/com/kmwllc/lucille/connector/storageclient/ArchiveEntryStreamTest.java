package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the stream handed to a FileHandler for an archive entry / compressed file: it must support bulk reads, and
 * closing it must not close the underlying archive stream.
 */
public class ArchiveEntryStreamTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  /** A handler that reads its entire stream with bulk reads and then closes it, as real handlers do. */
  public static class RecordingFileHandler implements FileHandler {
    static final List<String> contents = new ArrayList<>();
    static final List<Boolean> bulkReadSupported = new ArrayList<>();

    public RecordingFileHandler(Config config) {}

    @Override
    public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
      try {
        // InputStream's default bulk read loops over read(), which is what we are guarding against.
        bulkReadSupported.add(inputStream.getClass().getMethod("read", byte[].class, int.class, int.class)
            .getDeclaringClass() != InputStream.class);
        contents.add(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
        inputStream.close();
      } catch (IOException | NoSuchMethodException e) {
        throw new FileHandlerException("failed", e);
      }
      return List.<Document>of().iterator();
    }

    @Override
    public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr)
        throws FileHandlerException {
      processFile(inputStream, pathStr);
    }

    @Override
    public Spec getSpec() {
      return SpecBuilder.fileHandler().build();
    }
  }

  @Before
  public void clear() {
    RecordingFileHandler.contents.clear();
    RecordingFileHandler.bulkReadSupported.clear();
  }

  private void traverse(Path dir) throws Exception {
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run1", "pipeline1");
    Config config = ConfigFactory.parseMap(Map.of(
        "fileHandlers", Map.of("dat", Map.of("class", RecordingFileHandler.class.getName())),
        "fileOptions", Map.of("handleArchivedFiles", true, "handleCompressedFiles", true)));
    LocalStorageClient client = new LocalStorageClient();
    client.init();
    client.traverse(publisher, new TraversalParams(config, URI.create(dir.toString()), ""));
    client.shutdown();
  }

  @Test
  public void testMultiEntryZipUsesBulkReadsAndStaysOpen() throws Exception {
    Path dir = tempFolder.newFolder().toPath();
    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(dir.resolve("multi.zip")))) {
      for (String name : List.of("a.dat", "b.dat", "c.dat")) {
        zos.putNextEntry(new ZipEntry(name));
        zos.write(("contents of " + name).getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }

    traverse(dir);

    assertEquals(3, RecordingFileHandler.contents.size());
    assertTrue(RecordingFileHandler.contents.contains("contents of a.dat"));
    assertTrue(RecordingFileHandler.contents.contains("contents of b.dat"));
    assertTrue(RecordingFileHandler.contents.contains("contents of c.dat"));
    assertFalse(RecordingFileHandler.bulkReadSupported.contains(false));
  }

  @Test
  public void testGzipUsesBulkReads() throws Exception {
    Path dir = tempFolder.newFolder().toPath();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bytes)) {
      gz.write("zipped contents".getBytes(StandardCharsets.UTF_8));
    }
    Files.write(dir.resolve("file.dat.gz"), bytes.toByteArray());

    traverse(dir);

    assertEquals(List.of("zipped contents"), RecordingFileHandler.contents);
    assertEquals(List.of(true), RecordingFileHandler.bulkReadSupported);
  }
}
