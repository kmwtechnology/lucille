package com.kmwllc.lucille.video.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

/**
 * The res10 Caffe weights are not in the test resources, so these tests cover configuration,
 * missing files, and a file OpenCV cannot load as a network.
 */
public class FaceDetectDNNTest {

  @Test
  public void testRequiresPrototxtAndModelPaths() {
    try {
      new FaceDetectDNN(StageImages.config());
      fail("prototxtPath and modelPath are required");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("prototxtPath"));
      assertTrue(expected.getMessage().contains("modelPath"));
    }

    try {
      new FaceDetectDNN(StageImages.config("prototxtPath", "deploy.prototxt"));
      fail("modelPath is required");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("modelPath"));
    }
  }

  @Test
  public void testOptionalSettingsAreAcceptedBeforeTheFilesAreChecked() throws Exception {
    FaceDetectDNN stage = new FaceDetectDNN(StageImages.config(
        "prototxtPath", "deploy.prototxt",
        "modelPath", "res10.caffemodel",
        "confidence", 0.8,
        "boxField", "face",
        "draw", true));
    stage.stop();
    stage.stop();

    try {
      stage.start();
      fail("missing prototxt should be rejected");
    } catch (StageException expected) {
      assertTrue(expected.getMessage().contains("prototxtPath"));
      assertTrue(expected.getMessage().contains("deploy.prototxt"));
    }
  }

  @Test
  public void testStartRejectsAMissingModelAndABlankPath() throws Exception {
    Path prototxt = Files.createTempFile("lucille-face", ".prototxt");
    Path directory = Files.createTempDirectory("lucille-face-dir");
    Files.writeString(prototxt, "name: \"placeholder\"\n");
    try {
      FaceDetectDNN missingModel = new FaceDetectDNN(StageImages.config(
          "prototxtPath", prototxt.toString(),
          "modelPath", prototxt.resolveSibling("missing.caffemodel").toString()));
      try {
        missingModel.start();
        fail("missing caffemodel should be rejected");
      } catch (StageException expected) {
        assertTrue(expected.getMessage().contains("modelPath"));
        assertTrue(expected.getMessage().contains("missing.caffemodel"));
      } finally {
        missingModel.stop();
      }

      FaceDetectDNN blank = new FaceDetectDNN(StageImages.config(
          "prototxtPath", prototxt.toString(),
          "modelPath", "  "));
      try {
        blank.start();
        fail("blank modelPath should be rejected");
      } catch (StageException expected) {
        assertTrue(expected.getMessage().contains("modelPath"));
      } finally {
        blank.stop();
      }

      FaceDetectDNN directoryPath = new FaceDetectDNN(StageImages.config(
          "prototxtPath", directory.toString(),
          "modelPath", prototxt.toString()));
      try {
        directoryPath.start();
        fail("a directory is not a prototxt file");
      } catch (StageException expected) {
        assertTrue(expected.getMessage().contains("prototxtPath"));
      } finally {
        directoryPath.stop();
      }
    } finally {
      Files.deleteIfExists(prototxt);
      Files.deleteIfExists(directory);
    }
  }

  @Test
  public void testStartRejectsAFileThatIsNotACaffeNetwork() throws Exception {
    Path prototxt = Files.createTempFile("lucille-face", ".prototxt");
    Path model = Files.createTempFile("lucille-face", ".caffemodel");
    Files.writeString(prototxt, "this is not a caffe prototxt\n");
    Files.write(model, new byte[] {1, 2, 3, 4, 5});
    FaceDetectDNN stage = new FaceDetectDNN(StageImages.config(
        "prototxtPath", prototxt.toString(),
        "modelPath", model.toString()));
    try {
      stage.start();
      fail("OpenCV should reject the stand-in network files");
    } catch (StageException expected) {
      assertTrue(expected.getMessage().contains("Unable to load"));
    } catch (RuntimeException expected) {
      assertNotNull(expected.getMessage());
      assertFalse(expected.getMessage().isBlank());
    } finally {
      stage.stop();
      Files.deleteIfExists(prototxt);
      Files.deleteIfExists(model);
    }
  }

  @Test
  public void testProcessDocumentReportsAMissingModelAndKeepsTheImage() throws Exception {
    byte[] source = StageImages.gray8(32, 24, (x, y) -> 180);
    Document doc = StageImages.frame(source);
    doc.setField("opencv_box", "stale");
    FaceDetectDNN stage = new FaceDetectDNN(StageImages.config(
        "prototxtPath", "missing.prototxt",
        "modelPath", "missing.caffemodel",
        "boxField", "face"));
    try {
      stage.processDocument(doc);
      fail("processing should load the model and fail");
    } catch (StageException expected) {
      assertTrue(expected.getMessage().contains("does not exist"));
      assertTrue(expected.getMessage().contains("missing.prototxt"));
    } finally {
      stage.stop();
    }
    assertArrayEquals(source, doc.getBytes("frame_image"));
    assertEquals("stale", doc.getString("opencv_box"));
    assertFalse(doc.has("face"));
  }

  @Test
  public void testMissingAndEmptyImagesDoNotLoadTheModel() throws Exception {
    FaceDetectDNN stage = new FaceDetectDNN(StageImages.config(
        "prototxtPath", "missing.prototxt",
        "modelPath", "missing.caffemodel"));
    try {
      Document missing = Document.create("missing");
      missing.setField("note", "keep");
      assertNull(stage.processDocument(missing));
      assertEquals("keep", missing.getString("note"));
      assertFalse(missing.has("opencv_box"));

      Document empty = Document.create("empty");
      empty.setField("frame_image", new byte[0]);
      empty.setField("opencv_box", "keep");
      assertNull(stage.processDocument(empty));
      assertEquals("keep", empty.getString("opencv_box"));
    } finally {
      stage.stop();
    }
  }
}
