package com.kmwllc.lucille.producer.datatype;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileInfoTest {
  private static final Path testFile = Path.of("./pom.xml");
  private static final BasicFileAttributes mockAttrs = mock(BasicFileAttributes.class);
  private static final Instant creationTime = Instant.EPOCH;
  private static final Instant lastModifiedTime = Instant.now();

  @BeforeClass
  public static void setupClass() {
    when(mockAttrs.size()).thenReturn(1234L);
    when(mockAttrs.lastModifiedTime()).thenReturn(FileTime.from(lastModifiedTime));
    when(mockAttrs.creationTime()).thenReturn(FileTime.from(creationTime));
  }

  @Test
  public void testFileLoadingWithFile() throws Exception {
    final FileInfo info = new FileInfo(testFile, mockAttrs, true);
    assertEquals(testFile.toString(), info.getFilePath());

    final MessageDigest digest = MessageDigest.getInstance("MD5");
    final Base64.Encoder encoder = Base64.getEncoder();
    final String id = encoder.encodeToString(digest.digest(testFile.toString().getBytes(StandardCharsets.UTF_8)));
    assertEquals(id, info.getId());

    assertTrue(info.hasFileContent());
  }

  @Test
  public void testSerialization() throws Exception {
    final FileInfo info = new FileInfo(testFile, mockAttrs, true);
    assertEquals(testFile.toString(), info.getFilePath());
    assertTrue(info.hasFileContent());

    final ObjectMapper mapper = new ObjectMapper();
    byte[] bytes = mapper.writeValueAsBytes(info);
    final FileInfo info2 = mapper.readValue(bytes, FileInfo.class);
    assertEquals(info2, info);
  }
}
