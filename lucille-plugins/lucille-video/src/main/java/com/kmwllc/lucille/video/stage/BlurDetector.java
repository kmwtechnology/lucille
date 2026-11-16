package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_core.meanStdDev;
import static org.bytedeco.opencv.global.opencv_imgproc.Laplacian;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.bytedeco.javacpp.indexer.DoubleIndexer;
import org.bytedeco.opencv.global.opencv_core;
import org.bytedeco.opencv.opencv_core.Mat;

/**
 * Estimates sharpness as the variance of the Laplacian. Lower scores are blurrier.
 * Corresponds to MyRobotLab {@code OpenCVFilterBlurDetector}. Writes {@code scoreField} and {@code blurryField}.
 */
public class BlurDetector extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec()
      .optionalNumber("threshold")
      .optionalString("scoreField", "blurryField")
      .build();

  private final double threshold;
  private final String scoreField;
  private final String blurryField;

  public BlurDetector(Config config) {
    super(config);
    this.threshold = ConfigUtils.getDoubleOrDefault(config, "threshold", 100.0);
    this.scoreField = ConfigUtils.getOrDefault(config, "scoreField", "blur_score");
    this.blurryField = ConfigUtils.getOrDefault(config, "blurryField", "blurry");
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Mat gray = OpenCvImages.toGray(image);
    Mat lap = new Mat();
    Mat mean = new Mat();
    Mat stdDev = new Mat();
    try {
      Laplacian(gray, lap, opencv_core.CV_64F);
      meanStdDev(lap, mean, stdDev);
      double sigma;
      try (DoubleIndexer indexer = stdDev.createIndexer()) {
        sigma = indexer.get(0, 0);
      }
      double score = sigma * sigma;
      doc.setField(scoreField, score);
      doc.setField(blurryField, score <= threshold);
      return null;
    } finally {
      stdDev.close();
      mean.close();
      lap.close();
      gray.close();
    }
  }
}
