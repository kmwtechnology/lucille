package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_core.NORM_MINMAX;
import static org.bytedeco.opencv.global.opencv_core.normalize;
import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imencode;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imread;
import static org.bytedeco.opencv.global.opencv_imgproc.COLOR_BGR2GRAY;
import static org.bytedeco.opencv.global.opencv_imgproc.COLOR_BGRA2BGR;
import static org.bytedeco.opencv.global.opencv_imgproc.COLOR_BGRA2GRAY;
import static org.bytedeco.opencv.global.opencv_imgproc.COLOR_GRAY2BGR;
import static org.bytedeco.opencv.global.opencv_imgproc.cvtColor;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import java.util.List;
import java.util.ArrayList;
import java.util.List;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;
import org.bytedeco.opencv.opencv_core.Scalar;
import org.bytedeco.opencv.opencv_core.Size;

/**
 * Decode, encode, and annotate the image bytes a video stage stores on a document.
 */
final class OpenCvImages {

  private OpenCvImages() {
  }

  static Mat decode(byte[] bytes) {
    Mat encoded = new Mat(1, bytes.length, org.bytedeco.opencv.global.opencv_core.CV_8U);
    try {
      encoded.data().put(bytes);
      return imdecode(encoded, IMREAD_UNCHANGED);
    } finally {
      encoded.close();
    }
  }

  static byte[] encode(Mat image, String format) throws StageException {
    String extension = format.startsWith(".") ? format : "." + format;
    BytePointer buffer = new BytePointer();
    try {
      if (!imencode(extension, image, buffer)) {
        throw new StageException("Unable to encode image as " + extension);
      }
      int size = (int) buffer.limit();
      byte[] out = new byte[size];
      buffer.get(out);
      return out;
    } finally {
      buffer.deallocate();
    }
  }

  static Mat readFile(String path) {
    return imread(path, IMREAD_UNCHANGED);
  }

  static Mat toGray(Mat src) {
    if (src.channels() == 1) {
      return src.clone();
    }
    Mat gray = new Mat();
    int code = src.channels() == 4 ? COLOR_BGRA2GRAY : COLOR_BGR2GRAY;
    cvtColor(src, gray, code);
    return gray;
  }

  static Mat toBgr(Mat src) {
    if (src.channels() == 3) {
      return src.clone();
    }
    Mat bgr = new Mat();
    int code = src.channels() == 1 ? COLOR_GRAY2BGR : COLOR_BGRA2BGR;
    cvtColor(src, bgr, code);
    return bgr;
  }

  static Mat evenSize(Mat src) {
    int width = src.cols() & ~1;
    int height = src.rows() & ~1;
    if (width == src.cols() && height == src.rows()) {
      return src.clone();
    }
    return new Mat(src, new Rect(0, 0, Math.max(2, width), Math.max(2, height))).clone();
  }

  static List<String> pointStrings(Mat points) {
    List<String> encoded = new ArrayList<>();
    for (float[] point : pointPairs(points)) {
      encoded.add(point[0] + "," + point[1]);
    }
    return encoded;
  }

  static List<float[]> pointPairs(Mat points) {
    List<float[]> pairs = new ArrayList<>();
    if (points == null || points.empty()) {
      return pairs;
    }
    int channels = Math.max(1, points.channels());
    int count = channels == 2 ? (int) points.total() : (int) (points.total() / 2);
    Mat view = points.reshape(2, count);
    try (FloatIndexer indexer = view.createIndexer()) {
      for (int i = 0; i < count; i++) {
        pairs.add(new float[] {indexer.get(i, 0, 0), indexer.get(i, 0, 1)});
      }
    } finally {
      if (view != points) {
        view.close();
      }
    }
    return pairs;
  }

  static String box(Rect rect) {
    return rect.x() + "," + rect.y() + "," + rect.width() + "," + rect.height();
  }

  static Rect parseBox(String value) {
    String[] parts = value.split(",");
    if (parts.length != 4) {
      return null;
    }
    try {
      return new Rect(
          Integer.parseInt(parts[0].trim()),
          Integer.parseInt(parts[1].trim()),
          Integer.parseInt(parts[2].trim()),
          Integer.parseInt(parts[3].trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  static void replaceStrings(Document doc, String field, List<String> values) {
    doc.removeField(field);
    if (values.isEmpty()) {
      return;
    }
    doc.setField(field, values.get(0));
    for (int i = 1; i < values.size(); i++) {
      doc.addToField(field, values.get(i));
    }
  }

  static void replaceInts(Document doc, String field, int[] values) {
    doc.removeField(field);
    if (values.length == 0) {
      return;
    }
    doc.setField(field, values[0]);
    for (int i = 1; i < values.length; i++) {
      doc.addToField(field, values[i]);
    }
  }

  static int oddAtLeast(int value, int minimum) {
    int size = Math.max(minimum, value);
    return (size % 2 == 0) ? size + 1 : size;
  }

  static Size sizeOf(Mat image) {
    return new Size(image.cols(), image.rows());
  }

  /**
   * Single-channel 8-bit view of {@code image}. A gray 8-bit source is returned as-is.
   * Wider depths are normalized into a new 8-bit matrix.
   */
  static Mat toEightBit(Mat image) {
    if (image.channels() == 1 && image.depth() == org.bytedeco.opencv.global.opencv_core.CV_8U) {
      return image;
    }
    Mat gray = image.channels() == 1 ? image.clone() : toGray(image);
    Mat eight = new Mat();
    try {
      normalize(gray, eight, 0, 255, NORM_MINMAX, org.bytedeco.opencv.global.opencv_core.CV_8U, null);
      return eight;
    } finally {
      if (gray != image) {
        gray.close();
      }
    }
  }

  static void inRange(Mat source, Scalar lower, Scalar upper, Mat destination) {
    Mat lowerMat = new Mat(source.size(), source.type(), lower);
    Mat upperMat = new Mat(source.size(), source.type(), upper);
    try {
      org.bytedeco.opencv.global.opencv_core.inRange(source, lowerMat, upperMat, destination);
    } finally {
      upperMat.close();
      lowerMat.close();
    }
  }
}
