package com.kmwllc.lucille.message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import org.junit.Test;

public class KafkaDocumentDeserializerTest {

  // 15 MiB of bytes base64-encodes to 20,054,016 chars, just over Jackson's default
  // StreamReadConstraints maxStringLength of 20,000,000. A 14 MiB payload fits under the default.
  private static final int LARGE_BYTES_LEN = 15 * 1024 * 1024;

  private static byte[] patternedBytes(int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) {
      bytes[i] = (byte) (i * 31);
    }
    return bytes;
  }

  @Test
  public void testDeserializeLargeByteArrayField() throws Exception {
    byte[] content = patternedBytes(LARGE_BYTES_LEN);
    Document doc = Document.create("doc1");
    doc.setField("file_content", content);

    byte[] serialized = new KafkaDocumentSerializer().serialize("topic", doc);
    Document deserialized = new KafkaDocumentDeserializer().deserialize("topic", serialized);

    assertTrue(deserialized instanceof KafkaDocument);
    assertEquals("doc1", deserialized.getId());
    byte[] roundTripped = deserialized.getBytes("file_content");
    assertEquals(LARGE_BYTES_LEN, roundTripped.length);
    assertArrayEquals(content, roundTripped);
  }

  @Test
  public void testCreateFromJsonLargeByteArrayField() throws Exception {
    byte[] content = patternedBytes(LARGE_BYTES_LEN);
    Document doc = Document.create("doc2");
    doc.setField("file_content", content);

    Document parsed = Document.createFromJson(doc.toString());

    assertEquals("doc2", parsed.getId());
    byte[] roundTripped = parsed.getBytes("file_content");
    assertEquals(LARGE_BYTES_LEN, roundTripped.length);
    assertArrayEquals(content, roundTripped);
  }

  @Test
  public void testDeserializeNull() {
    assertNull(new KafkaDocumentDeserializer().deserialize("topic", null));
  }
}
