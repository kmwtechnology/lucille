package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_core.CV_8UC1;
import static org.bytedeco.opencv.global.opencv_objdetect.DICT_4X4_50;
import static org.bytedeco.opencv.global.opencv_objdetect.generateImageMarker;
import static org.bytedeco.opencv.global.opencv_objdetect.getPredefinedDictionary;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import java.util.List;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;
import org.bytedeco.opencv.opencv_core.Scalar;
import org.bytedeco.opencv.opencv_objdetect.Dictionary;
import org.bytedeco.opencv.opencv_objdetect.QRCodeEncoder;
import org.junit.Test;

public class QrCodeTest {

  @Test
  public void testReadsQrTextAndBox() throws Exception {
    Document doc = qrDocument("lucille");
    StageImageDebug.apply(new QrCode(StageImages.config("destField", "unused")), doc);

    assertEquals("lucille", doc.getString("qr_text"));
    assertFalse(doc.has("unused"));
    int[] box = box(doc.getString("qr_box"));
    assertTrue(box[0] >= 0 && box[1] >= 0);
    assertTrue(box[2] > 20 && box[3] > 20);
    assertTrue(box[0] + box[2] <= 240);
    assertTrue(box[1] + box[3] <= 240);
  }

  @Test
  public void testCustomSourceAndTextFields() throws Exception {
    byte[] code = qrDocument("stage-qr").getBytes("frame_image");
    byte[] blank = StageImages.gray8(240, 240, (x, y) -> 255);
    Document doc = StageImages.frame(blank);
    doc.setField("photo", code);
    doc.setField("code", "stale");
    StageImageDebug.apply(new QrCode(StageImages.config(
        "sourceField", "photo", "textField", "code", "boxField", "code_box")), doc);

    assertEquals("stage-qr", doc.getString("code"));
    assertFalse(doc.has("qr_text"));
    assertFalse(doc.has("qr_box"));
    assertTrue(doc.has("code_box"));
    assertArrayEquals(blank, doc.getBytes("frame_image"));
    assertArrayEquals(code, doc.getBytes("photo"));
  }

  @Test
  public void testClearsStaleTextWhenNothingDecodes() throws Exception {
    byte[] blank = StageImages.gray8(80, 80, (x, y) -> 255);
    Document doc = StageImages.frame(blank);
    doc.setField("qr_text", "stale");
    doc.setField("qr_box", "1,2,3,4");
    StageImageDebug.apply(new QrCode(StageImages.config()), doc);

    assertFalse(doc.has("qr_text"));
    assertFalse(doc.has("qr_box"));
    assertArrayEquals(blank, doc.getBytes("frame_image"));
  }

  @Test
  public void testQrModeIgnoresArucoMarkers() throws Exception {
    Document doc = StageImages.frame(arucoMarker(7));
    doc.setField("aruco_id", "stale");
    doc.setField("qr_text", "stale");
    doc.setField("qr_box", "1,2,3,4");
    StageImageDebug.apply(new QrCode(StageImages.config("mode", "qr")), doc);

    assertEquals("stale", doc.getString("aruco_id"));
    assertFalse(doc.has("qr_text"));
    assertFalse(doc.has("qr_box"));
  }

  @Test
  public void testReadsArucoIdsPerFrame() throws Exception {
    QrCode stage = new QrCode(StageImages.config("mode", "Aruco", "dictionary", "DICT_4X4_50"));
    Document pair = StageImages.frame(arucoPair());
    StageImageDebug.apply(stage, pair);
    List<String> ids = pair.getStringList("aruco_id");
    assertEquals(2, ids.size());
    assertTrue(ids.contains("7"));
    assertTrue(ids.contains("11"));

    Document next = StageImages.frame(arucoMarker(3));
    next.setField("aruco_id", "stale");
    StageImageDebug.apply(stage, next);
    assertEquals("3", next.getString("aruco_id"));
    assertEquals(1, next.getStringList("aruco_id").size());
  }

  @Test
  public void testUnknownDictionaryFallsBackAndADifferentDictionaryDoesNot() throws Exception {
    byte[] marker = arucoMarker(7);
    Document fallback = StageImages.frame(marker);
    StageImageDebug.apply(new QrCode(StageImages.config(
        "mode", "aruco", "dictionary", "not-a-dictionary", "markerField", "marker")), fallback);
    assertEquals("7", fallback.getString("marker"));
    assertFalse(fallback.has("aruco_id"));

    Document other = StageImages.frame(marker);
    other.setField("aruco_id", "stale");
    StageImageDebug.apply(new QrCode(StageImages.config(
        "mode", "aruco", "dictionary", "DICT_6X6_50")), other);
    assertFalse(other.has("aruco_id"));
  }

  @Test
  public void testBothModeReadsWhicheverSymbolIsPresent() throws Exception {
    Document code = qrDocument("lucille");
    code.setField("aruco_id", "stale");
    StageImageDebug.apply(new QrCode(StageImages.config("mode", "BOTH")), code);
    assertEquals("lucille", code.getString("qr_text"));
    assertFalse(code.has("aruco_id"));

    Document marker = StageImages.frame(arucoMarker(7));
    marker.setField("qr_text", "stale");
    marker.setField("qr_box", "1,2,3,4");
    StageImageDebug.apply(new QrCode(StageImages.config("mode", "both", "dictionary", "DICT_4X4_50")), marker);
    assertEquals("7", marker.getString("aruco_id"));
    assertFalse(marker.has("qr_text"));
    assertFalse(marker.has("qr_box"));
  }

  @Test
  public void testMissingAndEmptyImagesLeaveSymbolsUnset() throws Exception {
    Document missing = Document.create("missing");
    assertNull(StageImageDebug.apply(new QrCode(StageImages.config()), missing));
    assertFalse(missing.has("qr_text"));
    assertFalse(missing.has("aruco_id"));

    Document empty = Document.create("empty");
    empty.setField("frame_image", new byte[0]);
    empty.setField("qr_text", "keep");
    assertNull(new QrCode(StageImages.config("mode", "both")).processDocument(empty));
    assertEquals("keep", empty.getString("qr_text"));
  }

  private static Document qrDocument(String text) throws Exception {
    QRCodeEncoder encoder = QRCodeEncoder.create();
    Mat code = new Mat();
    try {
      encoder.encode(text, code);
      Document doc = StageImages.frame(OpenCvImages.encode(code, "png"));
      StageImageDebug.apply(new Resize(StageImages.config("destWidth", 240, "destHeight", 240)), doc);
      return doc;
    } finally {
      code.close();
      encoder.close();
    }
  }

  private static byte[] arucoMarker(int id) throws Exception {
    Dictionary dictionary = getPredefinedDictionary(DICT_4X4_50);
    Mat marker = new Mat();
    Mat canvas = new Mat(240, 240, CV_8UC1, new Scalar(255));
    try {
      generateImageMarker(dictionary, id, 160, marker, 1);
      copyOnto(canvas, marker, 40, 40);
      return OpenCvImages.encode(canvas, "png");
    } finally {
      canvas.close();
      marker.close();
      dictionary.close();
    }
  }

  private static byte[] arucoPair() throws Exception {
    Dictionary dictionary = getPredefinedDictionary(DICT_4X4_50);
    Mat first = new Mat();
    Mat second = new Mat();
    Mat canvas = new Mat(180, 340, CV_8UC1, new Scalar(255));
    try {
      generateImageMarker(dictionary, 7, 120, first, 1);
      generateImageMarker(dictionary, 11, 120, second, 1);
      copyOnto(canvas, first, 30, 30);
      copyOnto(canvas, second, 190, 30);
      return OpenCvImages.encode(canvas, "png");
    } finally {
      canvas.close();
      second.close();
      first.close();
      dictionary.close();
    }
  }

  private static void copyOnto(Mat canvas, Mat marker, int x, int y) {
    Mat roi = new Mat(canvas, new Rect(x, y, marker.cols(), marker.rows()));
    try {
      marker.copyTo(roi);
    } finally {
      roi.close();
    }
  }

  private static int[] box(String value) {
    String[] parts = value.split(",");
    assertEquals(4, parts.length);
    return new int[] {
        Integer.parseInt(parts[0]),
        Integer.parseInt(parts[1]),
        Integer.parseInt(parts[2]),
        Integer.parseInt(parts[3])
    };
  }
}
