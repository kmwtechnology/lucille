package com.kmwllc.lucille.video;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class VideoFileHandlerTest {

  @Test
  public void testDefaultExtraction() throws Exception {
    Config config = ConfigFactory.empty();
    VideoFileHandler handler = new VideoFileHandler(config);

    String filePath = "src/test/resources/VideoFileHandlerTest/sample.mp4";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    assertTrue(docs.hasNext());
    Document first = docs.next();

    String leaf = FilenameUtils.getName(filePath);
    String stem = FilenameUtils.removeExtension(leaf);

    assertEquals(stem + "-f0", first.getId());

    assertEquals(filePath, first.getString("source"));
    assertEquals(leaf, first.getString("file_name"));

    Integer frameIndex = first.getInt("frame_index");
    assertNotNull(frameIndex);
    assertEquals(Integer.valueOf(0), frameIndex);

    Long frameTimeMs = first.getLong("frame_time_ms");
    assertNotNull(frameTimeMs);
    assertTrue(frameTimeMs >= 0L);

    assertNotNull(first.getString("frame_timecode"));

    Integer width = first.getInt("image_width");
    Integer height = first.getInt("image_height");
    assertNotNull(width);
    assertNotNull(height);
    assertTrue(width > 0);
    assertTrue(height > 0);

    byte[] imageBytes = first.getBytes("frame_image");
    assertNotNull(imageBytes);
    assertTrue(imageBytes.length > 0);
  }

  @Test
  public void testDocIdPrefixAndFormat() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "docIdPrefix", "vid_",
        "docIdFormat", "frames_%s_parent"
    ));
    VideoFileHandler handler = new VideoFileHandler(config);

    String filePath = "src/test/resources/VideoFileHandlerTest/sample.mp4";
    File file = new File(filePath);
    String leaf = FilenameUtils.getName(filePath);
    String stem = FilenameUtils.removeExtension(leaf);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    assertTrue(docs.hasNext());
    Document first = docs.next();

    String expectedParentId = "vid_" + String.format("frames_%s_parent", stem);
    assertEquals(expectedParentId + "-f0", first.getId());
  }

  @Test
  public void testFrameStride() throws Exception {
    String filePath = "src/test/resources/VideoFileHandlerTest/sample.mp4";
    File file = new File(filePath);

    Config defaultConfig = ConfigFactory.empty();
    VideoFileHandler defaultHandler = new VideoFileHandler(defaultConfig);

    List<Integer> defaultIndices = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(file)) {
      Iterator<Document> docs = defaultHandler.processFile(fis, filePath);
      while (docs.hasNext()) {
        Document doc = docs.next();
        Integer frameIndex = doc.getInt("frame_index");
        assertNotNull(frameIndex);
        defaultIndices.add(frameIndex);
      }
    }

    Config strideConfig = ConfigFactory.parseMap(Map.of("frameStride", 4));
    VideoFileHandler strideHandler = new VideoFileHandler(strideConfig);

    List<Integer> strideIndices = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(file)) {
      Iterator<Document> docs = strideHandler.processFile(fis, filePath);
      while (docs.hasNext()) {
        Document doc = docs.next();
        Integer frameIndex = doc.getInt("frame_index");
        assertNotNull(frameIndex);
        strideIndices.add(frameIndex);
      }
    }

    assertTrue(strideIndices.size() < defaultIndices.size());

    int maxComparisons = Math.min(strideIndices.size(), defaultIndices.size() / 4);

    for (int i = 0; i < maxComparisons; i++) {
      int expectedIndex = defaultIndices.get(i * 4);
      int actualIndex = strideIndices.get(i);
      assertEquals(expectedIndex, actualIndex);
    }
  }
}

