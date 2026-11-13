package com.kmwllc.lucille.video.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import java.awt.Color;
import org.junit.Test;

public class FlipTest {

  @Test
  public void testVerticalFlipMovesTheTopBarToTheBottom() throws Exception {
    Document doc = StageImages.frame(topBar());
    doc.setField("image_width", 80);
    doc.setField("image_height", 60);
    StageImageDebug.apply(new Flip(StageImages.config("flipCode", 0)), doc);

    int[] size = StageImages.size(doc.getBytes("frame_image"));
    assertEquals(80, size[0]);
    assertEquals(60, size[1]);
    assertEquals(3, size[2]);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 5, 55) < 40);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 5, 2) > 200);
    assertEquals(Integer.valueOf(80), doc.getInt("image_width"));
    assertEquals(Integer.valueOf(60), doc.getInt("image_height"));
  }

  @Test
  public void testHorizontalFlipMovesTheLeftBarToTheRight() throws Exception {
    Document doc = StageImages.frame(leftBar());
    StageImageDebug.apply(new Flip(StageImages.config("flipCode", 1)), doc);

    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 75, 20) < 40);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 2, 20) > 200);
  }

  @Test
  public void testNegativeFlipCodeMirrorsBothAxes() throws Exception {
    byte[] source = StageImages.draw(80, 60, graphics -> {
      graphics.setColor(Color.WHITE);
      graphics.fillRect(0, 0, 80, 60);
      graphics.setColor(Color.BLACK);
      graphics.fillRect(0, 0, 20, 16);
    });
    Document doc = StageImages.frame(source);
    StageImageDebug.apply(new Flip(StageImages.config("flipCode", -1)), doc);

    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 75, 55) < 40);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 4, 4) > 200);
  }

  @Test
  public void testDefaultFlipCodeIsVertical() throws Exception {
    byte[] source = topBar();
    Document implicit = StageImages.frame(source);
    Document explicit = StageImages.frame(source);
    StageImageDebug.apply(new Flip(StageImages.config()), implicit);
    StageImageDebug.apply(new Flip(StageImages.config("flipCode", 0)), explicit);

    assertEquals(
        StageImages.sample(explicit.getBytes("frame_image"), 5, 55),
        StageImages.sample(implicit.getBytes("frame_image"), 5, 55));
    assertTrue(StageImages.sample(implicit.getBytes("frame_image"), 5, 55) < 40);

    Document horizontal = StageImages.frame(source);
    StageImageDebug.apply(new Flip(StageImages.config("flipCode", 1)), horizontal);
    assertTrue(StageImages.sample(horizontal.getBytes("frame_image"), 5, 55) > 200);
  }

  @Test
  public void testReadsTheSourceFieldAndWritesTheDestField() throws Exception {
    byte[] source = topBar();
    Document doc = Document.create("frame");
    doc.setField("photo", source);
    StageImageDebug.apply(new Flip(StageImages.config(
        "sourceField", "photo", "destField", "mirror", "flipCode", 0)), doc);

    assertNull(doc.getBytes("frame_image"));
    assertArrayEquals(source, doc.getBytes("photo"));
    assertTrue(StageImages.sample(doc.getBytes("mirror"), 5, 55) < 40);
    assertTrue(StageImages.sample(doc.getBytes("mirror"), 5, 2) > 200);
  }

  @Test
  public void testMissingAndEmptyImagesAreLeftAlone() throws Exception {
    Document missing = Document.create("missing");
    missing.setField("caption", "keep");
    assertNull(StageImageDebug.apply(new Flip(StageImages.config("flipCode", 1)), missing));
    assertEquals("keep", missing.getString("caption"));
    assertNull(missing.getBytes("frame_image"));

    Document empty = Document.create("empty");
    empty.setField("frame_image", new byte[0]);
    assertNull(new Flip(StageImages.config()).processDocument(empty));
    assertEquals(0, empty.getBytes("frame_image").length);
  }

  private static byte[] topBar() {
    return StageImages.draw(80, 60, graphics -> {
      graphics.setColor(Color.WHITE);
      graphics.fillRect(0, 0, 80, 60);
      graphics.setColor(Color.BLACK);
      graphics.fillRect(0, 0, 80, 10);
    });
  }

  private static byte[] leftBar() {
    return StageImages.draw(80, 60, graphics -> {
      graphics.setColor(Color.WHITE);
      graphics.fillRect(0, 0, 80, 60);
      graphics.setColor(Color.BLACK);
      graphics.fillRect(0, 0, 10, 60);
    });
  }
}
