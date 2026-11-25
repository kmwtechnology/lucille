package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_dnn.blobFromImage;
import static org.bytedeco.opencv.global.opencv_dnn.readNetFromCaffe;
import static org.bytedeco.opencv.global.opencv_imgproc.LINE_8;
import static org.bytedeco.opencv.global.opencv_imgproc.rectangle;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.opencv.global.opencv_core;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;
import org.bytedeco.opencv.opencv_core.Scalar;
import org.bytedeco.opencv.opencv_core.Size;
import org.bytedeco.opencv.opencv_dnn.Net;

/**
 * Caffe SSD face detector (the res10 300x300 model). Corresponds to MyRobotLab {@code OpenCVFilterFaceDetectDNN}.
 */
public class FaceDetectDNN extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec()
      .requiredString("prototxtPath", "modelPath")
      .optionalNumber("confidence")
      .optionalString("boxField")
      .optionalBoolean("draw")
      .build();

  private final String prototxtPath;
  private final String modelPath;
  private final float confidence;
  private final String boxField;
  private final boolean draw;
  private Net net;

  public FaceDetectDNN(Config config) {
    super(config);
    this.prototxtPath = config.getString("prototxtPath");
    this.modelPath = config.getString("modelPath");
    this.confidence = ConfigUtils.getDoubleOrDefault(config, "confidence", 0.5).floatValue();
    this.boxField = ConfigUtils.getOrDefault(config, "boxField", "opencv_box");
    this.draw = ConfigUtils.getBooleanOrDefault(config, "draw", false);
  }

  @Override
  public void start() throws StageException {
    ModelFiles.require(prototxtPath, "FaceDetectDNN prototxtPath");
    ModelFiles.require(modelPath, "FaceDetectDNN modelPath");
    net = readNetFromCaffe(prototxtPath, modelPath);
    if (net.empty()) {
      throw new StageException("Unable to load face detector " + modelPath);
    }
  }

  @Override
  public void stop() {
    if (net != null) {
      net.close();
      net = null;
    }
  }

  @Override
  protected Mat apply(Mat image, Document doc) throws StageException {
    if (net == null) {
      start();
    }
    Mat bgr = OpenCvImages.toBgr(image);
    Mat blob = blobFromImage(bgr, 1.0, new Size(300, 300), new Scalar(104, 177, 123, 0), false, false, opencv_core.CV_32F);
    Mat detections = null;
    try {
      net.setInput(blob);
      detections = net.forward();
      List<Rect> faces = new ArrayList<>();
      try (FloatIndexer indexer = detections.createIndexer()) {
        int count = (int) detections.size(2);
        for (int i = 0; i < count; i++) {
          float score = indexer.get(0, 0, i, 2);
          if (score < confidence) {
            continue;
          }
          int x1 = Math.round(indexer.get(0, 0, i, 3) * bgr.cols());
          int y1 = Math.round(indexer.get(0, 0, i, 4) * bgr.rows());
          int x2 = Math.round(indexer.get(0, 0, i, 5) * bgr.cols());
          int y2 = Math.round(indexer.get(0, 0, i, 6) * bgr.rows());
          x1 = Math.max(0, x1);
          y1 = Math.max(0, y1);
          int width = Math.max(1, Math.min(bgr.cols() - x1, x2 - x1));
          int height = Math.max(1, Math.min(bgr.rows() - y1, y2 - y1));
          faces.add(new Rect(x1, y1, width, height));
        }
      }
      List<String> encoded = new ArrayList<>();
      for (Rect face : faces) {
        encoded.add(OpenCvImages.box(face));
      }
      OpenCvImages.replaceStrings(doc, boxField, encoded);
      if (!draw || faces.isEmpty()) {
        return null;
      }
      for (Rect face : faces) {
        rectangle(bgr, face, new Scalar(0, 255, 0, 0), 2, LINE_8, 0);
      }
      Mat drawn = bgr.clone();
      return drawn;
    } finally {
      if (detections != null) {
        detections.close();
      }
      blob.close();
      bgr.close();
    }
  }
}
