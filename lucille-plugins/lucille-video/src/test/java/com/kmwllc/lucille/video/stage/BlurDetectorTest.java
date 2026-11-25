package com.kmwllc.lucille.video.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import org.junit.Test;

public class BlurDetectorTest {

  @Test
  public void testSharpFrameScoresHigherThanASmoothedFrame() throws Exception {
    byte[] source = StageImages.gray8(160, 120, (x, y) ->
        x > 30 && x < 130 && y > 20 && y < 100 ? 255 : 0);
    Document sharp = StageImages.frame(source);
    StageImageDebug.apply(new BlurDetector(StageImages.config()), sharp);

    Document smoothed = StageImages.frame(source);
    StageImageDebug.apply(new Smooth(StageImages.config("kernel", 31)), smoothed);
    StageImageDebug.apply(new BlurDetector(StageImages.config()), smoothed);

    double sharpScore = sharp.getDouble("blur_score");
    double smoothedScore = smoothed.getDouble("blur_score");
    assertTrue("sharp=" + sharpScore + " smoothed=" + smoothedScore, sharpScore > smoothedScore);
    assertTrue(sharpScore > 100);
    assertEquals(Boolean.FALSE, sharp.getBoolean("blurry"));
  }

  @Test
  public void testFlatFrameIsBlurryUntilTheThresholdDropsBelowTheScore() throws Exception {
    byte[] flat = StageImages.gray8(40, 30, (x, y) -> 128);
    Document doc = StageImages.frame(flat);
    StageImageDebug.apply(new BlurDetector(StageImages.config()), doc);

    assertTrue(doc.getDouble("blur_score") < 1.0);
    assertEquals(Boolean.TRUE, doc.getBoolean("blurry"));

    Document loose = StageImages.frame(flat);
    StageImageDebug.apply(new BlurDetector(StageImages.config("threshold", -1.0)), loose);
    assertTrue(loose.getDouble("blur_score") < 1.0);
    assertEquals(Boolean.FALSE, loose.getBoolean("blurry"));
  }

  @Test
  public void testCustomFieldsLeaveTheImageUntouched() throws Exception {
    byte[] source = StageImages.gray8(80, 60, (x, y) -> x < 40 ? 0 : 255);
    Document doc = StageImages.frame(source);
    doc.setField("image_width", 80);
    StageImageDebug.apply(new BlurDetector(StageImages.config(
        "scoreField", "sharpness",
        "blurryField", "is_blurry",
        "destField", "unused",
        "threshold", 1.0E9)), doc);

    assertArrayEquals(source, doc.getBytes("frame_image"));
    assertFalse(doc.has("unused"));
    assertFalse(doc.has("blur_score"));
    assertFalse(doc.has("blurry"));
    assertTrue(doc.getDouble("sharpness") > 0);
    assertEquals(Boolean.TRUE, doc.getBoolean("is_blurry"));
    assertEquals(Integer.valueOf(80), doc.getInt("image_width"));
  }

  @Test
  public void testMissingAndEmptyImagesAreNotScored() throws Exception {
    Document missing = Document.create("missing");
    missing.setField("note", "keep");
    assertNull(StageImageDebug.apply(new BlurDetector(StageImages.config()), missing));
    assertEquals("keep", missing.getString("note"));
    assertFalse(missing.has("blur_score"));
    assertFalse(missing.has("blurry"));

    Document empty = Document.create("empty");
    empty.setField("frame_image", new byte[0]);
    empty.setField("blur_score", 12.0);
    assertNull(new BlurDetector(StageImages.config()).processDocument(empty));
    assertEquals(Double.valueOf(12.0), empty.getDouble("blur_score"));
    assertEquals(0, empty.getBytes("frame_image").length);
  }
}
