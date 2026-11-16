package com.kmwllc.lucille.video.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import org.junit.Test;

public class AffineTest {

  @Test
  public void testShiftsTheCornerBlockByDxAndDy() throws Exception {
    byte[] source = StageImages.gray8(40, 40, (x, y) -> x < 12 && y < 12 ? 255 : 0);
    Document doc = StageImages.frame(source);
    StageImageDebug.apply(new Affine(StageImages.config("angle", 0, "dx", 18, "dy", 14)), doc);

    int[] size = StageImages.size(doc.getBytes("frame_image"));
    assertEquals(40, size[0]);
    assertEquals(40, size[1]);
    assertEquals(1, size[2]);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 22, 18) > 200);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 4, 4) < 40);
  }

  @Test
  public void testRotatesHalfATurnAroundTheCenter() throws Exception {
    byte[] source = StageImages.gray8(40, 40, (x, y) -> x < 12 && y < 12 ? 255 : 0);
    Document doc = StageImages.frame(source);
    StageImageDebug.apply(new Affine(StageImages.config("angle", 180, "dx", 0, "dy", 0)), doc);

    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 34, 34) > 200);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 4, 4) < 40);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 34, 4) < 40);
  }

  @Test
  public void testZeroMotionKeepsTheCornerInPlace() throws Exception {
    byte[] source = StageImages.gray8(40, 40, (x, y) -> x < 12 && y < 12 ? 255 : 0);
    Document doc = StageImages.frame(source);
    StageImageDebug.apply(new Affine(StageImages.config()), doc);

    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 4, 4) > 200);
    assertTrue(StageImages.sample(doc.getBytes("frame_image"), 30, 30) < 40);
  }

  @Test
  public void testWritesTheDestFieldAndKeepsAStoredSize() throws Exception {
    byte[] source = StageImages.gray8(40, 40, (x, y) -> x < 12 && y < 12 ? 255 : 0);
    Document doc = StageImages.frame(source);
    doc.setField("image_width", 40);
    doc.setField("image_height", 40);
    StageImageDebug.apply(new Affine(StageImages.config("dx", 18, "dy", 0, "destField", "warped")), doc);

    assertArrayEquals(source, doc.getBytes("frame_image"));
    assertTrue(StageImages.sample(doc.getBytes("warped"), 22, 4) > 200);
    assertTrue(StageImages.sample(doc.getBytes("warped"), 4, 4) < 40);
    assertEquals(Integer.valueOf(40), doc.getInt("image_width"));
    assertEquals(Integer.valueOf(40), doc.getInt("image_height"));
  }

  @Test
  public void testMissingEmptyAndUndecodableImagesAreLeftAlone() throws Exception {
    Document missing = Document.create("missing");
    missing.setField("note", "keep");
    assertNull(StageImageDebug.apply(new Affine(StageImages.config("dx", 10)), missing));
    assertEquals("keep", missing.getString("note"));
    assertNull(missing.getBytes("frame_image"));

    Document empty = Document.create("empty");
    empty.setField("frame_image", new byte[0]);
    assertNull(new Affine(StageImages.config("dx", 10)).processDocument(empty));
    assertEquals(0, empty.getBytes("frame_image").length);

    byte[] junk = new byte[] {1, 2, 3, 4, 5};
    Document undecodable = Document.create("junk");
    undecodable.setField("frame_image", junk);
    assertNull(new Affine(StageImages.config("angle", 90)).processDocument(undecodable));
    assertArrayEquals(junk, undecodable.getBytes("frame_image"));
    assertFalse(undecodable.has("image_width"));
  }
}
