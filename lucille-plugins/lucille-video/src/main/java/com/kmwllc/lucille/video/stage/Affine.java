package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_imgproc.INTER_LINEAR;
import static org.bytedeco.opencv.global.opencv_imgproc.getRotationMatrix2D;
import static org.bytedeco.opencv.global.opencv_imgproc.warpAffine;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.bytedeco.javacpp.indexer.DoubleIndexer;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Point2f;

/**
 * Rotates the image around its center and then translates it by {@code dx} and {@code dy} pixels.
 * Corresponds to the affine rotation described by MyRobotLab {@code OpenCVFilterAffine}.
 * {@code angle} is in degrees, clockwise-negative as in OpenCV.
 */
public class Affine extends OpenCvImageStage {

  public static final Spec SPEC = imageSpec().optionalNumber("angle", "dx", "dy").build();

  private final double angle;
  private final double dx;
  private final double dy;

  public Affine(Config config) {
    super(config);
    this.angle = ConfigUtils.getDoubleOrDefault(config, "angle", 0.0);
    this.dx = ConfigUtils.getDoubleOrDefault(config, "dx", 0.0);
    this.dy = ConfigUtils.getDoubleOrDefault(config, "dy", 0.0);
  }

  @Override
  protected Mat apply(Mat image, Document doc) {
    Point2f center = new Point2f(image.cols() / 2.0f, image.rows() / 2.0f);
    Mat rotation = getRotationMatrix2D(center, angle, 1.0);
    try (DoubleIndexer indexer = rotation.createIndexer()) {
      indexer.put(0, 2, indexer.get(0, 2) + dx);
      indexer.put(1, 2, indexer.get(1, 2) + dy);
    }
    Mat warped = new Mat();
    warpAffine(image, warped, rotation, OpenCvImages.sizeOf(image), INTER_LINEAR,
        org.bytedeco.opencv.global.opencv_core.BORDER_CONSTANT, new org.bytedeco.opencv.opencv_core.Scalar(0, 0, 0, 0));
    rotation.close();
    center.close();
    return warped;
  }
}
