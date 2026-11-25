package com.kmwllc.lucille.video.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads an image from a document, lets a subclass transform it, and writes the result back.
 * A missing source field leaves the document unchanged. Each worker thread has its own stage
 * instance, so subclasses may keep per-stream state such as a background model.
 */
abstract class OpenCvImageStage extends Stage {

  private static final Logger log = LoggerFactory.getLogger(OpenCvImageStage.class);

  protected final String sourceField;
  protected final String destField;
  protected final String format;

  protected OpenCvImageStage(Config config) {
    super(config);
    this.sourceField = ConfigUtils.getOrDefault(config, "sourceField", "frame_image");
    String configuredDest = ConfigUtils.getOrDefault(config, "destField", null);
    this.destField = configuredDest != null ? configuredDest : defaultDestField(sourceField);
    this.format = ConfigUtils.getOrDefault(config, "format", "png");
  }

  /**
   * Shared config: {@code sourceField} (default {@code frame_image}), {@code destField}
   * (default the source field), and {@code format} (default {@code png}).
   */
  protected static SpecBuilder imageSpec() {
    return SpecBuilder.stage().optionalString("sourceField", "destField", "format");
  }

  protected String defaultDestField(String sourceField) {
    return sourceField;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    byte[] bytes = doc.getBytes(sourceField);
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    Mat src = OpenCvImages.decode(bytes);
    if (src == null || src.empty()) {
      if (src != null) {
        src.close();
      }
      log.warn("Field {} on document {} is not a decodable image", sourceField, doc.getId());
      return null;
    }
    Mat out = null;
    try {
      out = apply(src, doc);
      Mat shown = src;
      if (out != null && !out.empty()) {
        doc.setField(destField, OpenCvImages.encode(out, format));
        if (doc.has("image_width")) {
          doc.setField("image_width", out.cols());
        }
        if (doc.has("image_height")) {
          doc.setField("image_height", out.rows());
        }
        shown = out;
      }
      return children(doc, shown);
    } catch (StageException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StageException("OpenCV stage failed for document " + doc.getId(), e);
    } finally {
      if (out != null && out != src) {
        out.close();
      }
      src.close();
    }
  }

  /**
   * @return the image to store, or null to keep the source bytes
   */
  protected abstract Mat apply(Mat image, Document doc) throws StageException;

  protected Iterator<Document> children(Document doc, Mat image) throws StageException {
    return null;
  }
}
