package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_imgproc.INTER_AREA;
import static org.bytedeco.opencv.global.opencv_imgproc.resize;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;

/**
 * Resizes the image to a fixed width and height. Corresponds to MyRobotLab {@code OpenCVFilterResize}.
 * Defaults are 480 by 640. When {@code keepAspect} is true, the image is scaled to fit inside that box.
 */
public class Resize extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec()
      .optionalNumber("destWidth", "destHeight")
      .optionalBoolean("keepAspect")
      .build();

  private final int destWidth;
  private final int destHeight;
  private final boolean keepAspect;

  public Resize(Config config) {
    super(config);
    this.destWidth = Math.max(1, ConfigUtils.getIntOrDefault(config, "destWidth", 480));
    this.destHeight = Math.max(1, ConfigUtils.getIntOrDefault(config, "destHeight", 640));
    this.keepAspect = ConfigUtils.getBooleanOrDefault(config, "keepAspect", false);
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    int width = destWidth;
    int height = destHeight;
    if (keepAspect) {
      double scale = Math.min(destWidth / (double) image.cols(), destHeight / (double) image.rows());
      width = Math.max(1, (int) Math.round(image.cols() * scale));
      height = Math.max(1, (int) Math.round(image.rows() * scale));
    }
    Mat resized = new Mat();
    resize(image, resized, new Size(width, height), 0, 0, INTER_AREA);
    return resized;
  }
}
