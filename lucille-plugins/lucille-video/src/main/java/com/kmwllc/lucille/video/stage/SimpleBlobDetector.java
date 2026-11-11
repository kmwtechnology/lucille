package com.kmwllc.lucille.video.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.bytedeco.opencv.opencv_core.KeyPointVector;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_features2d.SimpleBlobDetector.Params;

/**
 * OpenCV simple blob detector. Corresponds to MyRobotLab {@code OpenCVFilterSimpleBlobDetector}.
 */
public class SimpleBlobDetector extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec()
      .optionalNumber("minArea", "maxArea", "minCircularity")
      .optionalString("pointField")
      .build();

  private final float minArea;
  private final float maxArea;
  private final float minCircularity;
  private final String pointField;

  public SimpleBlobDetector(Config config) {
    super(config);
    this.minArea = ConfigUtils.getDoubleOrDefault(config, "minArea", 25.0).floatValue();
    this.maxArea = ConfigUtils.getDoubleOrDefault(config, "maxArea", 5000.0).floatValue();
    this.minCircularity = ConfigUtils.getDoubleOrDefault(config, "minCircularity", 0.1).floatValue();
    this.pointField = ConfigUtils.getOrDefault(config, "pointField", "opencv_blob");
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Params params = new Params();
    params.filterByArea(true);
    params.minArea(minArea);
    params.maxArea(maxArea);
    params.filterByCircularity(true);
    params.minCircularity(minCircularity);
    org.bytedeco.opencv.opencv_features2d.SimpleBlobDetector detector =
        org.bytedeco.opencv.opencv_features2d.SimpleBlobDetector.create(params);
    KeyPointVector keypoints = new KeyPointVector();
    Mat gray = OpenCvImages.toGray(image);
    try {
      detector.detect(gray, keypoints);
      List<String> points = new ArrayList<>();
      for (long i = 0; i < keypoints.size(); i++) {
        float x = keypoints.get(i).pt().x();
        float y = keypoints.get(i).pt().y();
        if (x == 0 && y == 0) {
          continue;
        }
        points.add(x + "," + y);
      }
      OpenCvImages.replaceStrings(doc, pointField, points);
      return null;
    } finally {
      gray.close();
      keypoints.close();
      detector.close();
      params.close();
    }
  }
}
