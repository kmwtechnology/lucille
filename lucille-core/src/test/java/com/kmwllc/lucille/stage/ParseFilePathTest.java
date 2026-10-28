package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.spec.NumberProperty;
import com.kmwllc.lucille.core.spec.Property;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ParseFilePathTest {

  private final StageFactory factory = StageFactory.of(ParseFilePath.class);

  @Test
  public void testParseAllDefaults() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/allDefault.conf");
    Document doc1 = Document.create("doc1");
    // operating system dependent for which separator char is used.
    doc1.setField("file_path", "Z:"+File.separatorChar+"folder1"+File.separatorChar+"folder2"+File.separatorChar+"folder3"+File.separatorChar+"myfile.xml");
    
    stage.processDocument(doc1);

    assertEquals("myfile.xml", doc1.getString("filename"));
    assertEquals("XML", doc1.getString("file_extension"));
    assertEquals("Z:"+File.separatorChar+"folder1"+File.separatorChar+"folder2"+File.separatorChar+"folder3", doc1.getString("folder"));

    assertEquals(4, doc1.getStringList("file_paths").size());
    assertEquals("Z:", doc1.getStringList("file_paths").get(0));
    assertEquals("Z:"+File.separatorChar+"folder1", doc1.getStringList("file_paths").get(1));
    assertEquals("Z:"+File.separatorChar+"folder1"+File.separatorChar+"folder2", doc1.getStringList("file_paths").get(2));
    assertEquals("Z:"+File.separatorChar+"folder1"+File.separatorChar+"folder2"+File.separatorChar+"folder3", doc1.getStringList("file_paths").get(3));
  }

  @Test
  public void testNormalization() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/allDefault.conf");
    Document doc1 = Document.create("doc1");

    // Z:/folder1/./folder2/../folder3/myfile.xml
    doc1.setField("file_path",
        "Z:" + File.separatorChar
            + "folder1" + File.separatorChar
            + "." + File.separatorChar
            + "folder2" + File.separatorChar
            + ".." + File.separatorChar
            + "folder3" + File.separatorChar
            + "myfile.xml");

    stage.processDocument(doc1);

    assertEquals("myfile.xml", doc1.getString("filename"));
    assertEquals("XML", doc1.getString("file_extension"));
    assertEquals("Z:" + File.separatorChar + "folder1" + File.separatorChar + "folder3", doc1.getString("folder"));

    assertEquals(3, doc1.getStringList("file_paths").size());
    assertEquals("Z:", doc1.getStringList("file_paths").get(0));
    assertEquals("Z:" + File.separatorChar + "folder1", doc1.getStringList("file_paths").get(1));
    assertEquals("Z:" + File.separatorChar + "folder1" + File.separatorChar + "folder3", doc1.getStringList("file_paths").get(2));
  }

  @Test
  public void testNonDefaultPathField() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/pathHere.conf");
    Document doc1 = Document.create("doc1");

    doc1.setField("path_here", "Z:" + File.separatorChar + "folder1" + File.separatorChar + "myfile.csv");
    // Don't want this to be used...
    doc1.setField("file_path", "C:" + File.separatorChar + "badFolder" + File.separatorChar + "secrets.txt");

    stage.processDocument(doc1);

    assertEquals("myfile.csv", doc1.getString("filename"));
    assertEquals("CSV", doc1.getString("file_extension"));
    assertEquals("Z:" + File.separatorChar + "folder1", doc1.getString("folder"));
  }

  @Test
  public void testLowercaseFileExtension() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/lowercaseNoHierarchy.conf");
    Document doc1 = Document.create("doc1");
    doc1.setField("file_path", "Z:" + File.separatorChar + "folder1" + File.separatorChar + "myfile.csv");

    stage.processDocument(doc1);

    assertEquals("myfile.csv", doc1.getString("filename"));
    assertEquals("csv", doc1.getString("file_extension"));

    Document doc2 = Document.create("doc2");
    doc2.setField("file_path", "Z:" + File.separatorChar + "folder1" + File.separatorChar + "myfile.GLaD");

    stage.processDocument(doc2);

    assertEquals("myfile.GLaD", doc2.getString("filename"));
    assertEquals("GLaD", doc2.getString("file_extension"));
  }

  @Test
  public void testNoHierarchy() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/lowercaseNoHierarchy.conf");
    Document doc1 = Document.create("doc1");
    doc1.setField("file_path", "Z:" + File.separatorChar + "folder1" + File.separatorChar + "myfile.csv");

    stage.processDocument(doc1);

    assertFalse(doc1.has("file_paths"));
    assertEquals("myfile.csv", doc1.getString("filename"));
    assertEquals("csv", doc1.getString("file_extension"));
    assertEquals("Z:" + File.separatorChar + "folder1", doc1.getString("folder"));
  }

  // Since you'll be testing on either Unix or Windows, we run both the tests to ensure being on one
  // machine doesn't prevent you from handling file paths on the other.
  @Test
  public void testForceWindowsSeparator() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/windows.conf");
    Document doc1 = Document.create("doc1");
    // Z:\folder1\myfile.csv
    doc1.setField("file_path", "Z:\\folder1\\myfile.csv");

    stage.processDocument(doc1);
    assertEquals("myfile.csv", doc1.getString("filename"));
    assertEquals("Z:\\folder1", doc1.getString("folder"));
    assertEquals("Z:\\folder1\\myfile.csv", doc1.getString("path"));

    assertEquals(2, doc1.getStringList("file_paths").size());
    assertEquals("Z:", doc1.getStringList("file_paths").get(0));
    assertEquals("Z:\\folder1", doc1.getStringList("file_paths").get(1));

    Document doc2 = Document.create("doc2");
    doc2.setField("file_path", "Z:/folder1/myfile.csv");

    stage.processDocument(doc2);
    assertEquals("myfile.csv", doc2.getString("filename"));
    assertEquals("Z:\\folder1", doc2.getString("folder"));
    assertEquals("Z:\\folder1\\myfile.csv", doc2.getString("path"));

    assertEquals(2, doc2.getStringList("file_paths").size());
    assertEquals("Z:", doc2.getStringList("file_paths").get(0));
    assertEquals("Z:\\folder1", doc2.getStringList("file_paths").get(1));
  }

  @Test
  public void testForceUnixSeparator() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/unix.conf");
    Document doc1 = Document.create("doc1");
    doc1.setField("file_path", "Z:/folder1/myfile.csv");

    stage.processDocument(doc1);
    assertEquals("myfile.csv", doc1.getString("filename"));
    assertEquals("Z:/folder1", doc1.getString("folder"));
    assertEquals("Z:/folder1/myfile.csv", doc1.getString("path"));

    assertEquals(2, doc1.getStringList("file_paths").size());
    assertEquals("Z:", doc1.getStringList("file_paths").get(0));
    assertEquals("Z:/folder1", doc1.getStringList("file_paths").get(1));

    Document doc2 = Document.create("doc2");
    doc2.setField("file_path", "Z:\\folder1\\myfile.csv");

    stage.processDocument(doc2);
    assertEquals("myfile.csv", doc2.getString("filename"));
    assertEquals("Z:/folder1", doc2.getString("folder"));
    assertEquals("Z:/folder1/myfile.csv", doc2.getString("path"));

    assertEquals(2, doc2.getStringList("file_paths").size());
    assertEquals("Z:", doc2.getStringList("file_paths").get(0));
    assertEquals("Z:/folder1", doc2.getStringList("file_paths").get(1));
  }

  @Test
  public void testInvalidConf() {
    Map<String, String> confMap = Map.of("fileSep", "*");
    Config faultyConfig = ConfigFactory.parseMap(confMap);

    assertThrows(IllegalArgumentException.class,
        () -> new ParseFilePath(faultyConfig)
    );
  }

  @Test
  public void testEmptyDoc() throws StageException {
    Stage stage = factory.get("ParseFilePathTest/allDefault.conf");
    Document doc1 = Document.create("doc1");

    stage.processDocument(doc1);

    assertFalse(doc1.has("filename"));
    assertFalse(doc1.has("folder"));
    assertFalse(doc1.has("path"));
    assertFalse(doc1.has("file_extension"));
  }

  @Test
  public void testOutputMatchesLegacyImplementation() throws StageException {
    List<String> paths = equivalencePaths();

    for (String fileSep : List.of("/", "\\")) {
      for (boolean uppercaseExtension : List.of(true, false)) {
        for (boolean includeHierarchy : List.of(true, false)) {
          Stage stage = factory.get(Map.of("fileSep", fileSep, "uppercaseExtension", uppercaseExtension,
              "includeHierarchy", includeHierarchy));

          for (String path : paths) {
            String message = "fileSep=" + fileSep + ", uppercaseExtension=" + uppercaseExtension
                + ", includeHierarchy=" + includeHierarchy + ", path=" + path;
            Document expected = Document.create("doc");
            expected.setField("file_path", path);
            Document actual = expected.deepCopy();

            Class<?> expectedError = errorClass(() -> legacyProcessDocument(expected, fileSep, uppercaseExtension, includeHierarchy));
            Class<?> actualError = errorClass(() -> stage.processDocument(actual));

            // The legacy implementation normalized the path a second time to build the hierarchy. Normalization is not
            // idempotent for a few inputs (e.g. "./:/b" normalizes to ":/b", which then normalizes to null), so it threw a
            // NullPointerException for them. Normalizing once parses these paths instead.
            String normalized = FilenameUtils.normalizeNoEndSeparator(path, fileSep.equals("/"));
            if (includeHierarchy && normalized != null
                && FilenameUtils.normalizeNoEndSeparator(normalized, fileSep.equals("/")) == null) {
              assertEquals(message, NullPointerException.class, expectedError);
              assertEquals(message, null, actualError);
              continue;
            }

            assertEquals(message, expectedError, actualError);
            assertEquals(message, expected, actual);
          }
        }
      }
    }
  }

  private static List<String> equivalencePaths() {
    List<String> paths = new ArrayList<>(List.of(
        "", "/", "\\", ".", "..", "../x", "/a/../../x", "a", "/a", "a/", "/a/b/", "a//b///c.txt", "///a///b///",
        "/a/./b/../c/file.txt", "~/docs/file.tar.gz", ".hidden", "/dir/.hidden", "/dir/no_extension", "/dir/file.",
        "C:", "C:\\", "C:a.txt", "C:\\a\\b.txt", "Z:\\folder1\\folder2\\myfile.xml", "C:/mixed\\separators/file.txt",
        "\\\\server\\share\\dir\\file.txt", "//server/share/dir/file.txt",
        "s3://bucket", "s3://bucket/", "s3://bucket/a/b/file.txt", "file:///a/b/file.txt",
        "https://host:8080/a%20b/c%2Fd.txt?x=1#frag", "gs://bucket//double//slashes/file.txt",
        "/dir/caf\u00e9/\u6587\u4ef6.txt", "/dir/file.a\u2215b", "/dir/file.tx\nt", "/dir/a b/c+d.txt"));

    StringBuilder unixDeep = new StringBuilder();
    StringBuilder windowsDeep = new StringBuilder("C:");
    for (int i = 0; i < 500; i++) {
      unixDeep.append("/dir").append(i);
      windowsDeep.append("\\dir").append(i);
    }
    paths.add(unixDeep + "/file.txt");
    paths.add(windowsDeep + "\\file.txt");
    paths.add("s3://bucket" + unixDeep + "/file.txt");

    // random paths over an alphabet weighted towards separators, dots and drive/scheme characters
    Random random = new Random(980);
    String alphabet = "ab//\\\\..:~ %+\u00e9";
    for (int i = 0; i < 2000; i++) {
      StringBuilder path = new StringBuilder();
      int length = random.nextInt(40);
      for (int j = 0; j < length; j++) {
        path.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      paths.add(path.toString());
    }

    return paths;
  }

  private static Class<?> errorClass(ThrowingRunnable runnable) {
    try {
      runnable.run();
      return null;
    } catch (Exception e) {
      return e.getClass();
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  // The implementation before the hierarchy was built in O(depth), kept to check that the output is unchanged.
  private static void legacyProcessDocument(Document doc, String fileSep, boolean uppercaseExtension, boolean includeHierarchy) {
    String filePath = doc.getString("file_path");
    boolean useUnix = fileSep.equals("/");
    filePath = FilenameUtils.normalizeNoEndSeparator(filePath, useUnix);

    doc.addToField("filename", FilenameUtils.getName(filePath));
    doc.addToField("folder", FilenameUtils.getFullPathNoEndSeparator(filePath));
    doc.addToField("path", filePath);

    if (uppercaseExtension) {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath).toUpperCase());
    } else {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath));
    }

    if (includeHierarchy) {
      String[] paths = StringUtils.split(FilenameUtils.normalizeNoEndSeparator(filePath, useUnix), fileSep);
      for (int i = 1; i < paths.length; i++) {
        String subPath = StringUtils.join(Arrays.copyOfRange(paths, 0, i), fileSep);
        doc.addToField("file_paths", subPath);
      }
    }
  }

  @Test
  public void testDeepPathBenchmark() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/"));
    StringBuilder deepPath = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      deepPath.append("/dir").append(i);
    }
    String path = deepPath + "/file.txt";

    Document expected = Document.create("doc");
    expected.setField("file_path", path);
    Document actual = expected.deepCopy();
    legacyProcessDocument(expected, "/", true, true);
    stage.processDocument(actual);
    assertEquals(expected, actual);
    assertEquals(2000, actual.getStringList("file_paths").size());

    // best of several rounds, so a GC pause or JIT compilation in one round doesn't decide the result
    long legacyBest = Long.MAX_VALUE;
    long currentBest = Long.MAX_VALUE;
    for (int round = 0; round < 5; round++) {
      Document legacyDoc = Document.create("doc");
      legacyDoc.setField("file_path", path);
      long start = System.nanoTime();
      legacyProcessDocument(legacyDoc, "/", true, true);
      legacyBest = Math.min(legacyBest, System.nanoTime() - start);

      Document currentDoc = Document.create("doc");
      currentDoc.setField("file_path", path);
      start = System.nanoTime();
      stage.processDocument(currentDoc);
      currentBest = Math.min(currentBest, System.nanoTime() - start);
    }

    assertTrue("legacy: " + legacyBest + "ns, current: " + currentBest + "ns", currentBest < legacyBest);
  }
}
