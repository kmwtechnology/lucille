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
 * </ul>
 */

public class VideoFileHandler extends BaseFileHandler {
  public static final Spec SPEC = SpecBuilder.fileHandler()
      .optionalString("docIdPrefix", "docIdFormat")
      .optionalNumber("frameStride")
      .build();

  private static final Logger log = LoggerFactory.getLogger(VideoFileHandler.class);

  private final String docIdFormat;
  private final int frameStride;

  public VideoFileHandler(Config config) {
    super(config);

    this.docIdFormat = config.hasPath("docIdFormat") ? config.getString("docIdFormat") : null;
    this.frameStride = config.hasPath("frameStride") ? config.getInt("frameStride") : 1;
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    String parentId = buildParentId(pathStr);
    String fileName = FilenameUtils.getName(pathStr);

    return getDocumentIterator(inputStream, pathStr, parentId, fileName);
  }

  private Iterator<Document> getDocumentIterator(InputStream inputStream, String pathStr, String parentId, String fileName) throws FileHandlerException {
    FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputStream);
    grabber.setCloseInputStream(true);

    Java2DFrameConverter converter = new Java2DFrameConverter();

    try {
      grabber.start();
    } catch (Exception e) {
      throw new FileHandlerException("Unable to start FFmpegFrameGrabber for: " + pathStr, e);
    }

    return new Iterator<>() {
      private boolean closed = false;
      private Document nextDoc = null;
      private int videoFrameIndex = 0;

      private void closeAll() {
        if (closed) {
          return;
        }

        closed = true;

        try {
          grabber.stop();
        } catch (Exception ignored ) {}

        try {
          grabber.release();
        } catch (Exception ignored ) {}
      }

      private boolean preFetch() {
        if (closed || nextDoc != null) {
          return nextDoc != null;
        }

        try {
          while (true) {
            Frame frame = grabber.grabImage();

            if (frame == null) {
              closeAll();
              return false;
            }

            int index = videoFrameIndex++;
            if (frameStride > 1 && (index % frameStride) == 0) {
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
            doc.setField("source", pathStr);
            doc.setField("file_name", fileName);
            doc.setField("frame_index", index);
            doc.setField("frame_time_ms", timeMs);
            doc.setField("frame_timecode", formatTimecode(timeMs));
            doc.setField("image_width", image.getWidth());
            doc.setField("image_height", image.getHeight());
            doc.setField("frame_image", pngBytes);

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
      public boolean hasNext() {
        return preFetch();
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
    };
  }

  private String buildParentId(String pathStr) {
    String leaf = FilenameUtils.getName(pathStr);
    String stem = FilenameUtils.removeExtension(leaf);
    String raw = (docIdFormat != null) ? String.format(docIdFormat, stem) : stem;
    return docIdPrefix + raw;
  }

  private String formatTimecode(long millis) {
    long totalSeconds = millis / 1000;
    long ms = millis % 1000;
    long hours = totalSeconds / 3600;
    long minutes = (totalSeconds % 3600) / 60;
    long seconds = totalSeconds % 60;

    return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, ms);
  }
}