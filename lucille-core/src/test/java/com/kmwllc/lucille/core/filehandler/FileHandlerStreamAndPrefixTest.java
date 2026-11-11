package com.kmwllc.lucille.core.filehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

/**
 * Tests that <code>processFileAndPublish</code> closes its stream however it ends, and that every built-in handler applies
 * <code>docIdPrefix</code> through {@link BaseFileHandler#createDocId(String)}.
 */
public class FileHandlerStreamAndPrefixTest {

  private static final String CSV = "id,name\n1,a\n2,b\n";
  private static final String JSON = "{\"id\":\"1\",\"name\":\"a\"}\n{\"id\":\"2\",\"name\":\"b\"}\n";
  private static final String XML = "<Company><staff><id>1</id></staff><staff><id>2</id></staff></Company>";

  /** Records whether it was closed. */
  private static class TrackingStream extends ByteArrayInputStream {
    boolean closed = false;

    TrackingStream(String content) {
      super(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static FileHandler handler(String extension, Map<String, Object> config) {
    return FileHandler.create(extension, ConfigFactory.parseMap(Map.of(extension, config)));
  }

  private static FileHandler csv(Map<String, Object> extra) {
    Map<String, Object> config = new HashMap<>(Map.of("idField", "id"));
    config.putAll(extra);
    return handler("csv", config);
  }

  private static FileHandler json(Map<String, Object> extra) {
    Map<String, Object> config = new HashMap<>(Map.of("idField", "id"));
    config.putAll(extra);
    return handler("json", config);
  }

  private static FileHandler xml(Map<String, Object> extra) {
    Map<String, Object> config = new HashMap<>(Map.of("xmlRootPath", "/Company/staff", "xmlIdPath", "/Company/staff/id"));
    config.putAll(extra);
    return handler("xml", config);
  }

  private static List<String> publishedIds(FileHandler handler, String content, String path) throws Exception {
    List<String> ids = new ArrayList<>();
    Publisher publisher = mock(Publisher.class);
    doAnswer(inv -> ids.add(((Document) inv.getArgument(0)).getId())).when(publisher).publish(any());
    handler.processFileAndPublish(publisher, new TrackingStream(content), path);
    return ids;
  }

  private static void assertClosedWhenPublisherThrows(FileHandler handler, String content, String path) throws Exception {
    Publisher publisher = mock(Publisher.class);
    doThrow(new RuntimeException("publish failed")).when(publisher).publish(any());
    TrackingStream stream = new TrackingStream(content);

    assertThrows(FileHandlerException.class, () -> handler.processFileAndPublish(publisher, stream, path));
    assertTrue(stream.closed);
  }

  @Test
  public void testCsvStreamIsClosedWhenThePublisherThrows() throws Exception {
    assertClosedWhenPublisherThrows(csv(Map.of()), CSV, "test.csv");
  }

  @Test
  public void testJsonStreamIsClosedWhenThePublisherThrows() throws Exception {
    assertClosedWhenPublisherThrows(json(Map.of()), JSON, "test.jsonl");
  }

  @Test
  public void testXmlStreamIsClosedWhenThePublisherThrows() throws Exception {
    assertClosedWhenPublisherThrows(xml(Map.of()), XML, "test.xml");
  }

  @Test
  public void testStreamIsClosedWhenProcessFileThrows() {
    FileHandler handler = (inputStream, pathStr) -> {
      throw new FileHandlerException("can't set up");
    };
    TrackingStream stream = new TrackingStream("");

    assertThrows(FileHandlerException.class, () -> handler.processFileAndPublish(mock(Publisher.class), stream, "test.dat"));
    assertTrue(stream.closed);
  }

  @Test
  public void testStreamIsClosedWhenTheFileIsPublished() throws Exception {
    TrackingStream stream = new TrackingStream(CSV);
    csv(Map.of()).processFileAndPublish(mock(Publisher.class), stream, "test.csv");
    assertTrue(stream.closed);
  }

  @Test
  public void testEveryBuiltInHandlerAppliesThePrefix() throws Exception {
    Map<String, Object> prefix = Map.of("docIdPrefix", "p-");

    assertEquals(List.of("p-1", "p-2"), publishedIds(csv(prefix), CSV, "test.csv"));
    assertEquals(List.of("p-1", "p-2"), publishedIds(json(prefix), JSON, "test.jsonl"));
    assertEquals(List.of("p-1", "p-2"), publishedIds(xml(prefix), XML, "test.xml"));
  }

  @Test
  public void testCsvDefaultIdGetsThePrefix() throws Exception {
    FileHandler handler = handler("csv", Map.of("docIdPrefix", "p-"));

    String pathHash = DigestUtils.md5Hex("dir/test.csv");

    assertEquals(List.of("p-" + pathHash + "-1", "p-" + pathHash + "-2"), publishedIds(handler, CSV, "dir/test.csv"));
  }

  @Test
  public void testCreateDocIdRefusesABlankId() {
    BaseFileHandler handler = (BaseFileHandler) csv(Map.of("docIdPrefix", "p-"));

    assertEquals("p-x", handler.createDocId("x"));
    assertThrows(IllegalArgumentException.class, () -> handler.createDocId(" "));
    assertThrows(IllegalArgumentException.class, () -> handler.createDocId(null));
  }
}
