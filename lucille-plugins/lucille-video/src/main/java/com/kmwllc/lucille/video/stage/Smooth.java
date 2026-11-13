package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_imgproc.GaussianBlur;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;

/**
 * Gaussian blur. Corresponds to MyRobotLab {@code OpenCVFilterSmooth}. {@code kernel} defaults to 9 and is forced odd.
 */
public class Smooth extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec().optionalNumber("kernel").build();

  private final int kernel;

  public Smooth(Config config) {
    super(config);
    this.kernel = OpenCvImages.oddAtLeast(ConfigUtils.getIntOrDefault(config, "kernel", 9), 3);
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Mat blurred = new Mat();
    GaussianBlur(image, blurred, new Size(kernel, kernel), 0);
    return blurred;
  }
}
