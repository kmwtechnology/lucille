package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_objdetect.DICT_4X4_50;
import static org.bytedeco.opencv.global.opencv_objdetect.DICT_4X4_100;
import static org.bytedeco.opencv.global.opencv_objdetect.DICT_5X5_50;
import static org.bytedeco.opencv.global.opencv_objdetect.DICT_6X6_50;
import static org.bytedeco.opencv.global.opencv_objdetect.getPredefinedDictionary;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.indexer.IntIndexer;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.MatVector;
import org.bytedeco.opencv.opencv_objdetect.ArucoDetector;
import org.bytedeco.opencv.opencv_objdetect.DetectorParameters;
import org.bytedeco.opencv.opencv_objdetect.Dictionary;
import org.bytedeco.opencv.opencv_objdetect.QRCodeDetector;
import org.bytedeco.opencv.opencv_objdetect.RefineParameters;

/**
 * Decodes QR codes and, optionally, ArUco markers.
 * Corresponds to MyRobotLab {@code OpenCVFilterQrCode}. {@code mode} is {@code qr}, {@code aruco}, or {@code both}.
 */
public class QrCode extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec()
      .optionalString("mode", "dictionary", "textField", "boxField", "markerField")
      .build();

  private static final Map<String, Integer> DICTIONARIES = Map.of(
      "DICT_4X4_50", DICT_4X4_50,
      "DICT_4X4_100", DICT_4X4_100,
      "DICT_5X5_50", DICT_5X5_50,
      "DICT_6X6_50", DICT_6X6_50);

  private final String mode;
  private final int dictionaryId;
  private final String textField;
  private final String boxField;
  private final String markerField;
  private final QRCodeDetector qrDetector = new QRCodeDetector();
  private ArucoDetector arucoDetector;

  public QrCode(Config config) {
    super(config);
    this.mode = ConfigUtils.getOrDefault(config, "mode", "qr").toLowerCase(Locale.ROOT);
    String dictionary = ConfigUtils.getOrDefault(config, "dictionary", "DICT_4X4_50");
    this.dictionaryId = DICTIONARIES.getOrDefault(dictionary, DICT_4X4_50);
    this.textField = ConfigUtils.getOrDefault(config, "textField", "qr_text");
    this.boxField = ConfigUtils.getOrDefault(config, "boxField", "qr_box");
    this.markerField = ConfigUtils.getOrDefault(config, "markerField", "aruco_id");
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Mat bgr = OpenCvImages.toBgr(image);
    try {
      if ("qr".equals(mode) || "both".equals(mode)) {
        readQr(bgr, doc);
      }
      if ("aruco".equals(mode) || "both".equals(mode)) {
        readAruco(bgr, doc);
      }
      return null;
    } finally {
      bgr.close();
    }
  }

  private void readQr(Mat image, Document doc) {
    Mat points = new Mat();
    try {
      BytePointer decoded = qrDetector.detectAndDecode(image);
      qrDetector.detect(image, points);
      String text = decoded == null || decoded.isNull() ? "" : decoded.getString();
      if (text == null || text.isEmpty()) {
        doc.removeField(textField);
        doc.removeField(boxField);
        return;
      }
      doc.setField(textField, text);
      List<float[]> corners = OpenCvImages.pointPairs(points);
      if (corners.size() >= 4) {
        float minX = Float.MAX_VALUE;
        float minY = Float.MAX_VALUE;
        float maxX = 0;
        float maxY = 0;
        for (float[] corner : corners) {
          minX = Math.min(minX, corner[0]);
          minY = Math.min(minY, corner[1]);
          maxX = Math.max(maxX, corner[0]);
          maxY = Math.max(maxY, corner[1]);
        }
        doc.setField(boxField, (int) minX + "," + (int) minY + "," + (int) (maxX - minX) + "," + (int) (maxY - minY));
      }
    } finally {
      points.close();
    }
  }

  private void readAruco(Mat image, Document doc) {
    if (arucoDetector == null) {
      Dictionary dictionary = getPredefinedDictionary(dictionaryId);
      arucoDetector = new ArucoDetector(dictionary, new DetectorParameters(), new RefineParameters());
    }
    MatVector corners = new MatVector();
    Mat ids = new Mat();
    try {
      arucoDetector.detectMarkers(image, corners, ids);
      List<String> markers = new ArrayList<>();
      if (!ids.empty()) {
        try (IntIndexer indexer = ids.createIndexer()) {
          for (int i = 0; i < ids.total(); i++) {
            markers.add(Integer.toString(indexer.get(i)));
          }
        }
      }
      OpenCvImages.replaceStrings(doc, markerField, markers);
    } finally {
      ids.close();
      corners.close();
    }
  }
}
