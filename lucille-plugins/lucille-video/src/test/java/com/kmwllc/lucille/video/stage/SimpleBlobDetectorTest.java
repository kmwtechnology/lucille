package com.kmwllc.lucille.video.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.Document;
import java.util.List;
import org.junit.Test;

public class SimpleBlobDetectorTest {

  @Test
  public void testFindsADarkDiskNearItsCenter() throws Exception {
    byte[] png = StageImages.disks(120, 120, 255, new int[][] {{60, 60, 18, 0}});
    Document doc = StageImages.frame(png);
    StageImageDebug.apply(new SimpleBlobDetector(StageImages.config(
        "minArea", 100, "maxArea", 4000, "minCircularity", 0.6)), doc);

    List<String> points = doc.getStringList("opencv_blob");
    assertEquals(1, points.size());
    assertNear(points.get(0), 60, 60, 6);
    assertArrayEquals(png, doc.getBytes("frame_image"));
  }

  @Test
  public void testFindsEachDarkDisk() throws Exception {
    byte[] png = StageImages.disks(140, 120, 255, new int[][] {
        {40, 40, 16, 0},
        {100, 70, 14, 0}
    });
    Document doc = StageImages.frame(png);
    StageImageDebug.apply(new SimpleBlobDetector(StageImages.config(
        "minArea", 100, "maxArea", 4000, "minCircularity", 0.6)), doc);

    List<String> points = doc.getStringList("opencv_blob");
    assertEquals(2, points.size());
    assertOneNear(points, 40, 40);
    assertOneNear(points, 100, 70);
  }

  @Test
  public void testAreaLimitsDropDisksOutsideTheRange() throws Exception {
    // A full-frame white square is itself a blob: area 14400, circularity about 0.79,
    // and its centroid falls on the disk, so OpenCV accepts it as dark. The corner cut
    // keeps that field out of the area window under test.
    byte[] png = StageImages.gray8(120, 120, (x, y) -> {
      int dx = x - 95;
      int dy = y - 95;
      if (dx * dx + dy * dy <= 16 * 16) {
        return 0;
      }
      return x < 70 && y < 100 ? 0 : 255;
    });
    Document kept = StageImages.frame(png);
    StageImageDebug.apply(detector(100, 4000, 0.6), kept);
    assertEquals(1, kept.getStringList("opencv_blob").size());

    Document tooSmall = StageImages.frame(png);
    tooSmall.setField("opencv_blob", "stale");
    StageImageDebug.apply(detector(5000, 20000, 0.6), tooSmall);
    assertFalse(tooSmall.has("opencv_blob"));

    Document tooBig = StageImages.frame(png);
    tooBig.setField("opencv_blob", "stale");
    StageImageDebug.apply(detector(10, 50, 0.6), tooBig);
    assertFalse(tooBig.has("opencv_blob"));
  }

  @Test
  public void testCircularityKeepsADiskAndDropsASquare() throws Exception {
    byte[] disk = StageImages.disks(120, 120, 255, new int[][] {{60, 60, 18, 0}});
    Document round = StageImages.frame(disk);
    StageImageDebug.apply(detector(100, 4000, 0.6), round);
    assertEquals(1, round.getStringList("opencv_blob").size());
    assertNear(round.getString("opencv_blob"), 60, 60, 6);

    byte[] square = StageImages.gray8(120, 120, (x, y) ->
        x >= 42 && x < 78 && y >= 42 && y < 78 ? 0 : 255);
    Document strict = StageImages.frame(square);
    strict.setField("opencv_blob", "stale");
    StageImageDebug.apply(detector(100, 4000, 0.95), strict);
    assertFalse(strict.has("opencv_blob"));

    Document loose = StageImages.frame(square);
    StageImageDebug.apply(detector(100, 4000, 0.2), loose);
    assertOneNear(loose.getStringList("opencv_blob"), 60, 60);
  }

  @Test
  public void testBrightDiskAndBlankFrameProduceNoPoints() throws Exception {
    byte[] bright = StageImages.disks(120, 120, 0, new int[][] {{60, 60, 18, 255}});
    Document light = StageImages.frame(bright);
    light.setField("opencv_blob", "stale");
    StageImageDebug.apply(detector(100, 4000, 0.5), light);
    assertFalse(light.has("opencv_blob"));

    byte[] blank = StageImages.gray8(40, 40, (x, y) -> 255);
    Document empty = StageImages.frame(blank);
    empty.setField("opencv_blob", "stale");
    StageImageDebug.apply(new SimpleBlobDetector(StageImages.config()), empty);
    assertFalse(empty.has("opencv_blob"));
    assertArrayEquals(blank, empty.getBytes("frame_image"));
  }

  @Test
  public void testCustomPointFieldReplacesOnlyThatField() throws Exception {
    byte[] png = StageImages.disks(120, 120, 255, new int[][] {{60, 60, 18, 0}});
    Document doc = StageImages.frame(png);
    doc.setField("blob_xy", "stale");
    doc.setField("opencv_blob", "leave");
    StageImageDebug.apply(new SimpleBlobDetector(StageImages.config(
        "pointField", "blob_xy", "minArea", 100, "maxArea", 4000, "minCircularity", 0.6,
        "destField", "unused")), doc);

    assertFalse(doc.has("unused"));
    assertEquals("leave", doc.getString("opencv_blob"));
    List<String> points = doc.getStringList("blob_xy");
    assertEquals(1, points.size());
    assertNear(points.get(0), 60, 60, 6);
  }

  @Test
  public void testMissingAndEmptyImagesLeavePointsUnset() throws Exception {
    Document missing = Document.create("missing");
    assertNull(StageImageDebug.apply(new SimpleBlobDetector(StageImages.config()), missing));
    assertFalse(missing.has("opencv_blob"));

    Document empty = Document.create("empty");
    empty.setField("frame_image", new byte[0]);
    empty.setField("opencv_blob", "keep");
    assertNull(new SimpleBlobDetector(StageImages.config()).processDocument(empty));
    assertEquals("keep", empty.getString("opencv_blob"));
  }

  private static SimpleBlobDetector detector(double minArea, double maxArea, double minCircularity) {
    return new SimpleBlobDetector(StageImages.config(
        "minArea", minArea, "maxArea", maxArea, "minCircularity", minCircularity));
  }

  private static void assertOneNear(List<String> points, float x, float y) {
    assertNotNull(points);
    assertFalse(points.isEmpty());
    for (String point : points) {
      String[] parts = point.split(",");
      float px = Float.parseFloat(parts[0]);
      float py = Float.parseFloat(parts[1]);
      if (Math.abs(px - x) < 8 && Math.abs(py - y) < 8) {
        return;
      }
    }
    fail("no blob near " + x + "," + y + " in " + points);
  }

  private static void assertNear(String point, float x, float y, float tolerance) {
    String[] parts = point.split(",");
    assertEquals(2, parts.length);
    assertEquals(x, Float.parseFloat(parts[0]), tolerance);
    assertEquals(y, Float.parseFloat(parts[1]), tolerance);
  }
}
