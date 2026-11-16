package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_core.flip;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.bytedeco.opencv.opencv_core.Mat;

/**
 * Mirrors the image. {@code flipCode} 0 is upside-down, 1 is left-right, and -1 is both.
 * Corresponds to MyRobotLab {@code OpenCVFilterFlip}.
 */
public class Flip extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec().optionalNumber("flipCode").build();

  private final int flipCode;

  public Flip(Config config) {
    super(config);
    this.flipCode = ConfigUtils.getIntOrDefault(config, "flipCode", 0);
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Mat flipped = new Mat();
    flip(image, flipped, flipCode);
    return flipped;
  }
}
