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
  public void testDecodeFilePath() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/", "decodeFilePath", true));

    Document doc1 = Document.create("doc1");
    doc1.setField("file_path", "s3://bucket/my%20folder/caf%C3%A9.txt");
    stage.processDocument(doc1);
    assertEquals("s3://bucket/my folder/caf\u00e9.txt", doc1.getString("path"));
    assertEquals("caf\u00e9.txt", doc1.getString("filename"));
    assertEquals("s3://bucket/my folder", doc1.getString("folder"));
    assertEquals("TXT", doc1.getString("file_extension"));
    // the hierarchy splits on runs of separators, as without decodeFilePath
    assertEquals(List.of("s3:", "s3:/bucket", "s3:/bucket/my folder"), doc1.getStringList("file_paths"));

    // "://" is kept for an empty authority too, and ".." is resolved after the scheme
    Document doc2 = Document.create("doc2");
    doc2.setField("file_path", "file:///a/b/../c.txt");
    stage.processDocument(doc2);
    assertEquals("file:///a/c.txt", doc2.getString("path"));
    assertEquals("file:///a", doc2.getString("folder"));

    // "+" is kept, unlike form decoding
    Document doc3 = Document.create("doc3");
    doc3.setField("file_path", "/dir/c++%20notes.txt");
    stage.processDocument(doc3);
    assertEquals("/dir/c++ notes.txt", doc3.getString("path"));

    // an invalid escape sequence leaves the path undecoded
    Document doc4 = Document.create("doc4");
    doc4.setField("file_path", "/dir/100%.txt");
    stage.processDocument(doc4);
    assertEquals("/dir/100%.txt", doc4.getString("path"));
    assertEquals("100%.txt", doc4.getString("filename"));

    // a drive letter is not a URI scheme
    Document doc5 = Document.create("doc5");
    doc5.setField("file_path", "C://dir/a%20b.txt");
    stage.processDocument(doc5);
    assertEquals("C:/dir/a b.txt", doc5.getString("path"));
  }

  @Test
  public void testDecodeFilePathWindowsSeparator() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "\\", "decodeFilePath", true));
    Document doc = Document.create("doc");
    doc.setField("file_path", "C:%5Cmy%20folder%5Cfile.txt");
    stage.processDocument(doc);

    assertEquals("C:\\my folder\\file.txt", doc.getString("path"));
    assertEquals("C:\\my folder", doc.getString("folder"));
    assertEquals(List.of("C:", "C:\\my folder"), doc.getStringList("file_paths"));
  }

  @Test
  public void testDecodeFilePathDisabledByDefault() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/"));
    Document doc = Document.create("doc");
    doc.setField("file_path", "s3://bucket/my%20folder/file.txt");
    stage.processDocument(doc);

    assertEquals("s3:/bucket/my%20folder/file.txt", doc.getString("path"));
    assertEquals("s3:/bucket/my%20folder", doc.getString("folder"));
  }

  @Test
  public void testCreateRelativeHierarchy() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/", "createRelativeHierarchy", true));

    assertEquals(List.of("a", "a/b"), parse(stage, "s3://bucket/a/b/file.txt").getStringList("file_paths"));
    assertEquals(List.of("a"), parse(stage, "file:///a/file.txt").getStringList("file_paths"));
    assertEquals(List.of("a", "a/c"), parse(stage, "https://host:8080/a/b/../c/file.txt").getStringList("file_paths"));
    assertFalse(parse(stage, "s3://bucket/file.txt").has("file_paths"));
    assertFalse(parse(stage, "s3://bucket").has("file_paths"));
    assertFalse(parse(stage, "s3://bucket/../a/file.txt").has("file_paths"));

    // only the hierarchy is relative
    Document doc = parse(stage, "s3://bucket/a/b/file.txt");
    assertEquals("s3:/bucket/a/b/file.txt", doc.getString("path"));
    assertEquals("s3:/bucket/a/b", doc.getString("folder"));

    // paths that are not URIs are not affected
    assertEquals(List.of("x", "x/y"), parse(stage, "/x/y/file.txt").getStringList("file_paths"));
    assertEquals(List.of("C:", "C:/x"), parse(stage, "C://x/file.txt").getStringList("file_paths"));
  }

  @Test
  public void testCreateRelativeHierarchyWithDecodeAndWindowsSeparator() throws StageException {
    Stage decoding = factory.get(Map.of("fileSep", "/", "createRelativeHierarchy", true, "decodeFilePath", true));
    Document doc = parse(decoding, "s3://bucket/my%20folder/sub/file.txt");
    assertEquals(List.of("my folder", "my folder/sub"), doc.getStringList("file_paths"));
    assertEquals("s3://bucket/my folder/sub/file.txt", doc.getString("path"));

    Stage windows = factory.get(Map.of("fileSep", "\\", "createRelativeHierarchy", true));
    assertEquals(List.of("a", "a\\b"), parse(windows, "s3://bucket/a/b/file.txt").getStringList("file_paths"));
    assertEquals(List.of("a", "a\\b"), parse(windows, "s3://bucket\\a\\b\\file.txt").getStringList("file_paths"));
  }

  @Test
  public void testIncludeLevel() throws StageException {
    Stage unix = factory.get(Map.of("fileSep", "/", "includeLevel", true, "includeHierarchy", false));
    assertEquals(Integer.valueOf(3), parse(unix, "/a/b/file.txt").getInt("level"));
    assertEquals(Integer.valueOf(3), parse(unix, "a//b///file.txt/").getInt("level"));
    assertEquals(Integer.valueOf(1), parse(unix, "file.txt").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(unix, "/").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(unix, "").getInt("level"));
    assertEquals(Integer.valueOf(4), parse(unix, "s3://bucket/a/file.txt").getInt("level"));
    assertFalse(parse(unix, "/a/b/file.txt").has("file_paths"));

    Stage windows = factory.get(Map.of("fileSep", "\\", "includeLevel", true));
    assertEquals(Integer.valueOf(3), parse(windows, "C:\\a\\file.txt").getInt("level"));

    Stage relative = factory.get(Map.of("fileSep", "/", "includeLevel", true, "createRelativeHierarchy", true));
    assertEquals(Integer.valueOf(2), parse(relative, "s3://bucket/a/file.txt").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(relative, "s3://bucket").getInt("level"));

    Stage defaults = factory.get(Map.of("fileSep", "/"));
    assertFalse(parse(defaults, "/a/b/file.txt").has("level"));
  }

  @Test
  public void testLevelMatchesHierarchy() throws StageException {
    for (String fileSep : List.of("/", "\\")) {
      Stage stage = factory.get(Map.of("fileSep", fileSep, "includeLevel", true, "uppercaseExtension", false));
      for (String path : equivalencePaths()) {
        if (FilenameUtils.normalizeNoEndSeparator(path, fileSep.equals("/")) == null) {
          continue;
        }
        Document doc = parse(stage, path);
        int hierarchySize = doc.has("file_paths") ? doc.getStringList("file_paths").size() : 0;
        int level = doc.getInt("level");
        assertEquals("fileSep=" + fileSep + ", path=" + path, Math.max(level - 1, 0), hierarchySize);
      }
    }
  }

  @Test
  public void testClearInvalidExtension() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/", "clearInvalidExtension", true));

    for (char ch : "\u2215\u2044\u29F8\uFF0F\uFE68\u2216\u29F9\uFF3C\n\t\u0001\u007F\u009F".toCharArray()) {
      Document doc = parse(stage, "/dir/file.a" + ch + "b");
      assertEquals("U+" + Integer.toHexString(ch), "", doc.getString("file_extension"));
      assertEquals("file.a" + ch + "b", doc.getString("filename"));
    }

    assertEquals("TXT", parse(stage, "/dir/file.txt").getString("file_extension"));
    assertEquals("", parse(stage, "/dir/no_extension").getString("file_extension"));
    assertEquals("\u00C9T\u00C9", parse(stage, "/dir/file.\u00E9t\u00E9").getString("file_extension"));

    Stage lowercase = factory.get(Map.of("fileSep", "/", "clearInvalidExtension", true, "uppercaseExtension", false));
    assertEquals("", parse(lowercase, "/dir/file.a\uFF0Fb").getString("file_extension"));
    assertEquals("txt", parse(lowercase, "/dir/file.txt").getString("file_extension"));
  }

  @Test
  public void testInvalidExtensionKeptByDefault() throws StageException {
    Stage stage = factory.get(Map.of("fileSep", "/"));
    assertEquals("A\u2215B", parse(stage, "/dir/file.a\u2215b").getString("file_extension"));
  }

  private static Document parse(Stage stage, String filePath) throws StageException {
    Document doc = Document.create("doc");
    doc.setField("file_path", filePath);
    stage.processDocument(doc);
    return doc;
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
