package com.kmwllc.lucille.video.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.bytedeco.javacpp.indexer.UByteIndexer;
import org.bytedeco.opencv.global.opencv_core;
import org.bytedeco.opencv.opencv_core.Mat;

/**
 * Optional desktop preview for the OpenCV stage tests.
 *
 * <p>Leave it off for a normal run. Turn it on with {@code -Dopencv.stage.debug=true}
 * (Maven and the IDE) or with the {@code OPENCV_STAGE_DEBUG=true} environment variable.
 * Each stage then opens one window with the input image, the output image, and the
 * fields the stage wrote. Boxes, lines, points, and polygons are drawn on the output.
 * Close the window, or press Next, to continue the test.
 */
final class StageImageDebug {

  private static final AtomicInteger STEP = new AtomicInteger();
  private static final int TARGET_LONG_SIDE = 360;
  private static final int METADATA_LIMIT = 40;
  private static final Set<String> IMAGE_FIELDS = Set.of(
      "frame_image", "depth_preview", "split_image", "other", "opencv_full_image");
  private static final Color BOX_COLOR = new Color(40, 220, 80);
  private static final Color LINE_COLOR = new Color(255, 140, 40);
  private static final Color POINT_COLOR = new Color(80, 210, 255);
  private static final Color POLYGON_COLOR = new Color(255, 220, 60);
  
  

  private StageImageDebug() {
  }

  static boolean enabled() {
    return truthy(System.getProperty("opencv.stage.debug"))
        || truthy(System.getenv("OPENCV_STAGE_DEBUG"));
  }

  static Iterator<Document> apply(Stage stage, Document doc) throws StageException {
    byte[] input = doc.getBytes("frame_image");
    Iterator<Document> children = stage.processDocument(doc);
    show(stage.getClass().getSimpleName(), input, outputBytes(doc, input), doc);
    return children;
  }

  static void show(String title, byte[] input, byte[] output, Document doc) {
    if (!enabled() || input == null || input.length == 0) {
      return;
    }
    if (GraphicsEnvironment.isHeadless()) {
      throw new IllegalStateException("opencv.stage.debug is set, but this JVM has no desktop");
    }
    BufferedImage inputImage = read(input);
    BufferedImage outputImage = output == null ? null : read(output);
    String details = metadata(doc);
    String windowTitle = title + " (" + STEP.incrementAndGet() + ")";
    try {
      SwingUtilities.invokeAndWait(() -> open(windowTitle, inputImage, outputImage, doc, details));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while showing " + windowTitle, e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new IllegalStateException("Could not show " + windowTitle, cause);
    }
  }

  private static byte[] outputBytes(Document doc, byte[] input) {
    byte[] frame = doc.getBytes("frame_image");
    if (frame != null && !Arrays.equals(frame, input)) {
      return frame;
    }
    for (String field : new String[] {"depth_preview", "split_image", "other"}) {
      byte[] extra = doc.getBytes(field);
      if (extra != null && extra.length > 0) {
        return extra;
      }
    }
    return frame;
  }

  private static void open(String title, BufferedImage input, BufferedImage output, Document doc, String details) {
    double scale = scale(input, output);
    JDialog dialog = new JDialog((JFrame) null, title, true);
    dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
    JPanel images = new JPanel(new GridLayout(1, 2, 16, 0));
    images.setBackground(new Color(32, 32, 32));
    images.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
    images.add(panel("input", input, scale, null));
    images.add(panel("output", output, scale, doc));
    JTextArea text = new JTextArea(details);
    text.setEditable(false);
    text.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
    text.setLineWrap(false);
    JScrollPane fields = new JScrollPane(text);
    fields.setPreferredSize(new Dimension(380, 320));
    fields.setBorder(BorderFactory.createTitledBorder("extracted fields"));
    JButton next = new JButton("Next");
    next.addActionListener(event -> dialog.dispose());
    JPanel controls = new JPanel(new BorderLayout());
    JLabel hint = new JLabel("Input is on the left. Output is on the right, with boxes drawn on it.");
    hint.setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 12));
    controls.add(hint, BorderLayout.CENTER);
    controls.add(next, BorderLayout.EAST);
    controls.setBorder(BorderFactory.createEmptyBorder(0, 0, 12, 12));
    JPanel root = new JPanel(new BorderLayout());
    root.add(images, BorderLayout.CENTER);
    root.add(fields, BorderLayout.EAST);
    root.add(controls, BorderLayout.SOUTH);
    dialog.setContentPane(root);
    dialog.pack();
    dialog.setLocationRelativeTo(null);
    dialog.setAlwaysOnTop(true);
    dialog.setVisible(true);
  }

  private static JPanel panel(String caption, BufferedImage image, double scale, Document annotations) {
    JPanel panel = new JPanel(new BorderLayout(0, 8));
    panel.setOpaque(false);
    JLabel label = new JLabel(caption);
    label.setForeground(Color.WHITE);
    panel.add(label, BorderLayout.NORTH);
    if (image == null) {
      JLabel missing = new JLabel("no image");
      missing.setForeground(Color.LIGHT_GRAY);
      panel.add(missing, BorderLayout.CENTER);
      return panel;
    }
    BufferedImage shown = zoom(image, scale);
    if (annotations != null) {
      annotate(shown, annotations, scale);
    }
    JLabel picture = new JLabel(new javax.swing.ImageIcon(shown));
    picture.setBorder(BorderFactory.createLineBorder(Color.GRAY));
    panel.add(picture, BorderLayout.CENTER);
    return panel;
  }

  private static String metadata(Document doc) {
    if (doc == null) {
      return "No extracted fields.";
    }
    StringBuilder classification = new StringBuilder();
    StringBuilder detections = new StringBuilder();
    StringBuilder other = new StringBuilder();
    for (String name : doc.getFieldNames()) {
      if (IMAGE_FIELDS.contains(name)) {
        continue;
      }
      String rendered = renderField(doc, name);
      if (rendered == null) {
        continue;
      }
      if (classification(name)) {
        classification.append(rendered);
      } else if (geometry(name) != Geometry.NONE || name.startsWith("match_")) {
        detections.append(rendered);
      } else {
        other.append(rendered);
      }
    }
    StringBuilder text = new StringBuilder();
    appendSection(text, "Classification", classification);
    appendSection(text, "Detections", detections);
    appendSection(text, "Other", other);
    return text.length() == 0 ? "No extracted fields." : text.toString();
  }

  private static void appendSection(StringBuilder text, String title, StringBuilder body) {
    if (body.length() == 0) {
      return;
    }
    if (text.length() > 0) {
      text.append('\n');
    }
    text.append(title).append('\n').append(body);
  }

  private static boolean classification(String name) {
    String normalized = name.toLowerCase(Locale.ROOT);
    return normalized.contains("label")
        || normalized.contains("text")
        || normalized.contains("status")
        || normalized.contains("confidence")
        || normalized.contains("found")
        || normalized.contains("blur")
        || normalized.contains("sample")
        || normalized.contains("histogram")
        || normalized.contains("marker")
        || normalized.contains("aruco");
  }

  private static String renderField(Document doc, String name) {
    JsonNode node = doc.getJson(name);
    if (node != null && (node.isBinary() || (node.isArray() && node.size() > 0 && node.get(0).isBinary()))) {
      byte[] bytes = doc.getBytes(name);
      int length = bytes == null ? 0 : bytes.length;
      return name + ": " + length + " bytes\n";
    }
    List<String> values = doc.getStringList(name);
    if (values == null || values.isEmpty()) {
      return null;
    }
    if (values.size() == 1) {
      return name + ": " + values.get(0) + "\n";
    }
    StringBuilder text = new StringBuilder();
    text.append(name).append(" (").append(values.size()).append(")\n");
    int shown = Math.min(values.size(), METADATA_LIMIT);
    for (int i = 0; i < shown; i++) {
      text.append("  ").append(values.get(i)).append('\n');
    }
    if (values.size() > shown) {
      text.append("  ... ").append(values.size() - shown).append(" more\n");
    }
    return text.toString();
  }

  private static void annotate(BufferedImage image, Document doc, double scale) {
    Graphics2D graphics = image.createGraphics();
    try {
      graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
      graphics.setStroke(new BasicStroke(2f));
      graphics.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 13));
      List<Mark> boxes = new ArrayList<>();
      for (String name : doc.getFieldNames()) {
        Geometry kind = geometry(name);
        if (kind == Geometry.NONE) {
          continue;
        }
        List<String> values = doc.getStringList(name);
        if (values == null) {
          continue;
        }
        if (kind == Geometry.POLYGON) {
          drawPolygon(graphics, values, scale);
        } else if (kind == Geometry.POINT) {
          for (String value : values) {
            drawPoint(graphics, value, scale);
          }
        } else if (kind == Geometry.LINE) {
          for (String value : values) {
            drawLine(graphics, value, scale);
          }
        } else {
          for (String value : values) {
            Mark box = parseBox(value);
            if (box != null) {
              boxes.add(box);
            }
          }
        }
      }
      Mark match = matchBox(doc);
      if (match != null) {
        boxes.add(match);
      }
      if (boxes.size() == 1 && boxes.get(0).label == null) {
        boxes.get(0).label = caption(doc);
      }
      for (Mark box : boxes) {
        drawBox(graphics, box, scale);
      }
    } finally {
      graphics.dispose();
    }
  }

  private static String caption(Document doc) {
    String face = doc.getString("face_label");
    if (face != null && !face.isBlank()) {
      String confidence = doc.getString("face_confidence");
      return confidence == null ? "face " + face : "face " + face + " " + confidence;
    }
    for (String field : new String[] {"qr_text", "aruco_id", "motion_status"}) {
      String value = doc.getString(field);
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static Mark matchBox(Document doc) {
    Integer x = doc.getInt("match_x");
    Integer y = doc.getInt("match_y");
    Integer width = doc.getInt("match_width");
    Integer height = doc.getInt("match_height");
    if (x == null || y == null || width == null || height == null) {
      return null;
    }
    String score = doc.getString("match_score");
    return new Mark(score == null ? "match" : "match " + score, x, y, width, height);
  }

  private static Mark parseBox(String value) {
    String[] parts = value.split(",");
    if (parts.length < 4) {
      return null;
    }
    int start = 0;
    String label = null;
    if (number(parts[0]) == null) {
      label = parts[0].trim();
      start = 1;
    }
    if (parts.length - start < 4) {
      return null;
    }
    Double x = number(parts[start]);
    Double y = number(parts[start + 1]);
    Double width = number(parts[start + 2]);
    Double height = number(parts[start + 3]);
    if (x == null || y == null || width == null || height == null) {
      return null;
    }
    if (parts.length - start >= 5) {
      String score = parts[start + 4].trim();
      label = label == null ? score : label + " " + score;
    }
    return new Mark(label, x, y, width, height);
  }

  private static void drawBox(Graphics2D graphics, Mark box, double scale) {
    int x = (int) Math.round(box.x * scale);
    int y = (int) Math.round(box.y * scale);
    int width = Math.max(1, (int) Math.round(box.width * scale));
    int height = Math.max(1, (int) Math.round(box.height * scale));
    graphics.setColor(BOX_COLOR);
    graphics.drawRect(x, y, width, height);
    if (box.label != null && !box.label.isBlank()) {
      drawLabel(graphics, box.label, x, y);
    }
  }

  private static void drawLine(Graphics2D graphics, String value, double scale) {
    String[] parts = value.split(",");
    if (parts.length < 4) {
      return;
    }
    Double x1 = number(parts[0]);
    Double y1 = number(parts[1]);
    Double x2 = number(parts[2]);
    Double y2 = number(parts[3]);
    if (x1 == null || y1 == null || x2 == null || y2 == null) {
      return;
    }
    graphics.setColor(LINE_COLOR);
    graphics.drawLine(scaled(x1, scale), scaled(y1, scale), scaled(x2, scale), scaled(y2, scale));
  }

  private static void drawPoint(Graphics2D graphics, String value, double scale) {
    String[] parts = value.split(",");
    if (parts.length < 2) {
      return;
    }
    Double x = number(parts[0]);
    Double y = number(parts[1]);
    if (x == null || y == null) {
      return;
    }
    int sx = scaled(x, scale);
    int sy = scaled(y, scale);
    graphics.setColor(POINT_COLOR);
    graphics.fillOval(sx - 4, sy - 4, 8, 8);
  }

  private static void drawPolygon(Graphics2D graphics, List<String> values, double scale) {
    List<Integer> xs = new ArrayList<>();
    List<Integer> ys = new ArrayList<>();
    for (String value : values) {
      String[] parts = value.split(",");
      if (parts.length < 2) {
        continue;
      }
      Double x = number(parts[0]);
      Double y = number(parts[1]);
      if (x == null || y == null) {
        continue;
      }
      xs.add(scaled(x, scale));
      ys.add(scaled(y, scale));
      graphics.setColor(POINT_COLOR);
      int sx = xs.get(xs.size() - 1);
      int sy = ys.get(ys.size() - 1);
      graphics.fillOval(sx - 4, sy - 4, 8, 8);
    }
    if (xs.size() < 2) {
      return;
    }
    graphics.setColor(POLYGON_COLOR);
    for (int i = 0; i < xs.size(); i++) {
      int next = (i + 1) % xs.size();
      graphics.drawLine(xs.get(i), ys.get(i), xs.get(next), ys.get(next));
    }
  }

  private static void drawLabel(Graphics2D graphics, String label, int x, int y) {
    FontMetrics metrics = graphics.getFontMetrics();
    int width = metrics.stringWidth(label) + 6;
    int top = y - metrics.getAscent() - 4;
    if (top < 0) {
      top = y + 4;
    }
    graphics.setColor(new Color(0, 0, 0, 180));
    graphics.fillRect(x, top, width, metrics.getHeight());
    graphics.setColor(BOX_COLOR);
    graphics.drawString(label, x + 3, top + metrics.getAscent());
  }

  private static int scaled(double value, double scale) {
    return (int) Math.round(value * scale);
  }

  private static Double number(String token) {
    try {
      return Double.valueOf(token.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Geometry geometry(String name) {
    String normalized = name.toLowerCase(Locale.ROOT);
    if (normalized.contains("polygon")) {
      return Geometry.POLYGON;
    }
    if (normalized.contains("line")) {
      return Geometry.LINE;
    }
    if (normalized.contains("point") || normalized.contains("blob") || normalized.contains("track")) {
      return Geometry.POINT;
    }
    if (normalized.contains("box") || normalized.contains("region")) {
      return Geometry.BOX;
    }
    return Geometry.NONE;
  }

  private static double scale(BufferedImage input, BufferedImage output) {
    int longer = Math.max(longSide(input), longSide(output));
    if (longer <= 0 || longer >= TARGET_LONG_SIDE) {
      return 1;
    }
    return TARGET_LONG_SIDE / (double) longer;
  }

  private static int longSide(BufferedImage image) {
    if (image == null) {
      return 0;
    }
    return Math.max(image.getWidth(), image.getHeight());
  }

  private static BufferedImage zoom(BufferedImage source, double scale) {
    if (scale == 1) {
      return source;
    }
    int width = Math.max(1, (int) Math.round(source.getWidth() * scale));
    int height = Math.max(1, (int) Math.round(source.getHeight() * scale));
    BufferedImage scaled = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
    Graphics2D graphics = scaled.createGraphics();
    try {
      graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);
      graphics.drawImage(source, 0, 0, width, height, null);
    } finally {
      graphics.dispose();
    }
    return scaled;
  }

  private static BufferedImage read(byte[] png) {
    Mat decoded = OpenCvImages.decode(png);
    if (decoded == null || decoded.empty()) {
      if (decoded != null) {
        decoded.close();
      }
      return null;
    }
    Mat eight = decoded.depth() == opencv_core.CV_8U ? decoded : OpenCvImages.toEightBit(decoded);
    Mat bgr = OpenCvImages.toBgr(eight);
    try {
      return toImage(bgr);
    } finally {
      bgr.close();
      if (eight != decoded) {
        eight.close();
      }
      decoded.close();
    }
  }

  private static BufferedImage toImage(Mat bgr) {
    int width = bgr.cols();
    int height = bgr.rows();
    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
    int[] pixels = new int[width * height];
    try (UByteIndexer indexer = bgr.createIndexer()) {
      for (int y = 0; y < height; y++) {
        for (int x = 0; x < width; x++) {
          int blue = indexer.get(y, x, 0) & 0xff;
          int green = indexer.get(y, x, 1) & 0xff;
          int red = indexer.get(y, x, 2) & 0xff;
          pixels[y * width + x] = (red << 16) | (green << 8) | blue;
        }
      }
    }
    image.setRGB(0, 0, width, height, pixels, 0, width);
    return image;
  }

  private static boolean truthy(String value) {
    if (value == null) {
      return false;
    }
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    return "true".equals(normalized) || "1".equals(normalized) || "yes".equals(normalized);
  }

  private enum Geometry {
    NONE, BOX, LINE, POINT, POLYGON
  }

  private static final class Mark {
    private String label;
    private final double x;
    private final double y;
    private final double width;
    private final double height;

    private Mark(String label, double x, double y, double width, double height) {
      this.label = label;
      this.x = x;
      this.y = y;
      this.width = width;
      this.height = height;
    }
  }
}
