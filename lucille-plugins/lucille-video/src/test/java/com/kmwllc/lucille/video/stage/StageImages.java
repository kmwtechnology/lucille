package com.kmwllc.lucille.video.stage;

import static org.bytedeco.opencv.global.opencv_core.CV_8UC1;
import static org.bytedeco.opencv.global.opencv_imgproc.LINE_8;
import static org.bytedeco.opencv.global.opencv_imgproc.circle;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.imageio.ImageIO;
import org.bytedeco.javacpp.indexer.UByteIndexer;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Point;
import org.bytedeco.opencv.opencv_core.Scalar;

/**
 * Small frames and config maps shared by the OpenCV stage unit tests.
 */
final class StageImages {

  private StageImages() {
  }

  static Config config(Object... entries) {
    Map<String, Object> values = new HashMap<>();
    values.put("name", "test");
    for (int i = 0; i < entries.length; i += 2) {
      values.put((String) entries[i], entries[i + 1]);
    }
    return ConfigFactory.parseMap(values);
  }

  static Document frame(byte[] png) {
    Document doc = Document.create("frame");
    doc.setField("frame_image", png);
    return doc;
  }

  static byte[] draw(int width, int height, Consumer<Graphics2D> painter) {
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
    Graphics2D graphics = image.createGraphics();
    try {
      painter.accept(graphics);
    } finally {
      graphics.dispose();
    }
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ImageIO.write(image, "png", out);
      return out.toByteArray();
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static byte[] gray8(int width, int height, BiFunction<Integer, Integer, Integer> pixel) throws Exception {
    Mat mat = new Mat(height, width, CV_8UC1);
    try (UByteIndexer indexer = mat.createIndexer()) {
      for (int y = 0; y < height; y++) {
        for (int x = 0; x < width; x++) {
          indexer.put(y, x, pixel.apply(x, y));
        }
      }
      return OpenCvImages.encode(mat, "png");
    } finally {
      mat.close();
    }
  }

  /**
   * Gray image with filled circles. Each blob is {@code {cx, cy, radius, gray}}.
   */
  static byte[] disks(int width, int height, int background, int[][] blobs) throws Exception {
    Mat image = new Mat(height, width, CV_8UC1, new Scalar(background));
    try {
      for (int[] blob : blobs) {
        circle(image, new Point(blob[0], blob[1]), blob[2], new Scalar(blob[3]), -1, LINE_8, 0);
      }
      return OpenCvImages.encode(image, "png");
    } finally {
      image.close();
    }
  }

  static int[] size(byte[] png) {
    Mat image = OpenCvImages.decode(png);
    try {
      return new int[] {image.cols(), image.rows(), image.channels()};
    } finally {
      image.close();
    }
  }

  static int sample(byte[] png, int x, int y) {
    Mat image = OpenCvImages.decode(png);
    try (UByteIndexer indexer = image.createIndexer()) {
      if (image.channels() == 1) {
        return indexer.get(y, x) & 0xff;
      }
      return indexer.get(y, x, 0) & 0xff;
    } finally {
      image.close();
    }
  }
}
