package com.kmwllc.lucille.video;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.imageio.ImageIO;
import org.apache.commons.io.FilenameUtils;
import org.bytedeco.javacv.FFmpegFrameGrabber.Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;

/**
 * Extracts video frames using JavaCV and emits one Document per sampled frame. Each emitted document
 * contains PNG-compressed frame bytes and timing/size metadata, along with the original source path
 * and filename.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>docIdPrefix (String, Optional) : Prefix prepended to all generated document ids.</li>
 *   <li>docIdFormat (String, Optional) : The pattern applied to the filename stem when composing the
 *   parent id (a single %s placeholder).</li>
 *   <li>frameStride (Int, Optional) : Emit every Nth video frame; defaults to 1.</li>
 *   <li>sourceField (String, Optional) : Field used to store the original file path; defaults to "source".</li>
 *   <li>fileNameField (String, Optional) : Field used to store the leaf filename; defaults to "file_name".</li>
 *   <li>frameIndexField (String, Optional) : Field used to store the zero-based frame index; defaults to "frame_index".</li>
 *   <li>frameTimeMsField (String, Optional) : Field used to store the frame timestamp in milliseconds; defaults to "frame_time_ms".</li>
 *   <li>frameTimecodeField (String, Optional) : Field used to store the human-readable frame timecode; defaults to "frame_timecode".</li>
 *   <li>imageWidthField (String, Optional) : Field used to store the frame width in pixels; defaults to "image_width".</li>
 *   <li>imageHeightField (String, Optional) : Field used to store the frame height in pixels; defaults to "image_height".</li>
 *   <li>frameImageField (String, Optional) : Field used to store the PNG-encoded frame bytes; defaults to "frame_image".</li>
 * </ul>
 */
public class VideoFileHandler extends BaseFileHandler {
  public static final Spec SPEC = SpecBuilder.fileHandler()
      .optionalString("docIdPrefix", "docIdFormat", "sourceField", "fileNameField", "frameIndexField", "frameTimeMsField", "frameTimecodeField", "imageWidthField", "imageHeightField", "frameImageField")
      .optionalNumber("frameStride")
      .build();

  private static final Logger log = LoggerFactory.getLogger(VideoFileHandler.class);

  private final String docIdFormat;
  private final int frameStride;
  private final String sourceField;
  private final String fileNameField;
  private final String frameIndexField;
  private final String frameTimeMsField;
  private final String frameTimecodeField;
  private final String imageWidthField;
  private final String imageHeightField;
  private final String frameImageField;

  public VideoFileHandler(Config config) {
    super(config);

    this.docIdFormat = config.hasPath("docIdFormat") ? config.getString("docIdFormat") : null;
    this.frameStride = Math.max(1, config.hasPath("frameStride") ? config.getInt("frameStride") : 1);
    this.sourceField = config.hasPath("sourceField") ? config.getString("sourceField") : "source";
    this.fileNameField = config.hasPath("fileNameField") ? config.getString("fileNameField") : "file_name";
    this.frameIndexField = config.hasPath("frameIndexField") ? config.getString("frameIndexField") : "frame_index";
    this.frameTimeMsField = config.hasPath("frameTimeMsField") ? config.getString("frameTimeMsField") : "frame_time_ms";
    this.frameTimecodeField = config.hasPath("frameTimecodeField") ? config.getString("frameTimecodeField") : "frame_timecode";
    this.imageWidthField = config.hasPath("imageWidthField") ? config.getString("imageWidthField") : "image_width";
    this.imageHeightField = config.hasPath("imageHeightField") ? config.getString("imageHeightField") : "image_height";
    this.frameImageField = config.hasPath("frameImageField") ? config.getString("frameImageField") : "frame_image";
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    String parentId = buildParentId(pathStr);
    String fileName = FilenameUtils.getName(pathStr);

    return getDocumentIterator(inputStream, pathStr, parentId, fileName);
  }

  private Iterator<Document> getDocumentIterator(InputStream inputStream, String pathStr,
      String parentId, String fileName) {
    FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputStream);
    grabber.setCloseInputStream(true);

    Java2DFrameConverter converter = new Java2DFrameConverter();

    return new FrameDocumentIterator(
        grabber,
        converter,
        pathStr,
        parentId,
        fileName,
        frameStride,
        sourceField,
        fileNameField,
        frameIndexField,
        frameTimeMsField,
        frameTimecodeField,
        imageWidthField,
        imageHeightField,
        frameImageField);
  }

  private static class FrameDocumentIterator implements Iterator<Document> {

    private final FFmpegFrameGrabber grabber;
    private final Java2DFrameConverter converter;
    private final String pathStr;
    private final String parentId;
    private final String fileName;
    private final int frameStride;
    private final String sourceField;
    private final String fileNameField;
    private final String frameIndexField;
    private final String frameTimeMsField;
    private final String frameTimecodeField;
    private final String imageWidthField;
    private final String imageHeightField;
    private final String frameImageField;

    private boolean closed = false;
    private boolean started = false;
    private Document nextDoc = null;
    private int videoFrameIndex = 0;

    FrameDocumentIterator(FFmpegFrameGrabber grabber,
        Java2DFrameConverter converter,
        String pathStr,
        String parentId,
        String fileName,
        int frameStride,
        String sourceField,
        String fileNameField,
        String frameIndexField,
        String frameTimeMsField,
        String frameTimecodeField,
        String imageWidthField,
        String imageHeightField,
        String frameImageField) {
      this.grabber = grabber;
      this.converter = converter;
      this.pathStr = pathStr;
      this.parentId = parentId;
      this.fileName = fileName;
      this.frameStride = frameStride;
      this.sourceField = sourceField;
      this.fileNameField = fileNameField;
      this.frameIndexField = frameIndexField;
      this.frameTimeMsField = frameTimeMsField;
      this.frameTimecodeField = frameTimecodeField;
      this.imageWidthField = imageWidthField;
      this.imageHeightField = imageHeightField;
      this.frameImageField = frameImageField;
    }

    private void closeAll() {
      if (closed) {
        return;
      }
      closed = true;

      try {
        grabber.stop();
      } catch (Exception ignored) {}

      try {
        grabber.release();
      } catch (Exception ignored) {}
    }

    @Override
    public boolean hasNext() {
      if (closed || nextDoc != null) {
        return nextDoc != null;
      }

      if (!started) {
        try {
          grabber.start();
          started = true;
        } catch (Exception e) {
          closeAll();
          throw new RuntimeException("Unable to start FFmpegFrameGrabber for: " + pathStr, e);
        }
      }

      try {
        while (true) {
          Frame frame = grabber.grabImage();

          if (frame == null) {
            closeAll();
            return false;
          }

          int index = videoFrameIndex++;
          if (frameStride > 1 && (index % frameStride) != 0) {
            continue;
          }

          BufferedImage image = converter.convert(frame);
          if (image == null) {
            continue;
          }

          long timeUs = grabber.getTimestamp();
          long timeMs = timeUs / 1000L;

          byte[] pngBytes = null;
          try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            ImageIO.write(image, "png", stream);
            stream.flush();
            pngBytes = stream.toByteArray();
          } catch (IOException e) {
            log.warn("PNG encoding failed for {} frame {}: {}", pathStr, index, e.toString());
          }

          String frameId = parentId + "-f" + index;
          Document doc = Document.create(frameId);
          doc.setField(sourceField, pathStr);
          doc.setField(fileNameField, fileName);
          doc.setField(frameIndexField, index);
          doc.setField(frameTimeMsField, timeMs);
          doc.setField(frameTimecodeField, formatTimecode(timeMs));
          doc.setField(imageWidthField, image.getWidth());
          doc.setField(imageHeightField, image.getHeight());
          doc.setField(frameImageField, pngBytes);

          nextDoc = doc;
          return true;
        }
      } catch (Exception e) {
        closeAll();
        log.error("Error while grabbing frames for {}", pathStr, e);
        return false;
      }
    }

    @Override
    public Document next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Document out = nextDoc;
      nextDoc = null;
      return out;
    }
  }

  private String buildParentId(String pathStr) {
    String leaf = FilenameUtils.getName(pathStr);
    String stem = FilenameUtils.removeExtension(leaf);
    String raw = (docIdFormat != null) ? String.format(docIdFormat, stem) : stem;
    return docIdPrefix + raw;
  }

  private static String formatTimecode(long millis) {
    long totalSeconds = millis / 1000;
    long ms = millis % 1000;
    long hours = totalSeconds / 3600;
    long minutes = (totalSeconds % 3600) / 60;
    long seconds = totalSeconds % 60;

    return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, ms);
  }
}