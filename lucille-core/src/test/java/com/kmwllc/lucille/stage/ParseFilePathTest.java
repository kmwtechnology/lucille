package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
    Map<String, String> confMap = Map.of("fileSeparator", "*");
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
  public void testParseUriFileConnectorForms() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/", "includeLevel", true));

    Document s3 = parse(stage, "s3://acme-docs/legal/2026/contract.docx");
    assertEquals("s3", s3.getString("scheme"));
    assertEquals("acme-docs", s3.getString("authority"));
    assertEquals("contract.docx", s3.getString("filename"));
    assertEquals("/legal/2026", s3.getString("folder"));
    assertEquals("/legal/2026/contract.docx", s3.getString("path"));
    assertEquals("DOCX", s3.getString("file_extension"));
    assertEquals(List.of("legal", "legal/2026"), s3.getStringList("file_paths"));
    assertEquals(Integer.valueOf(3), s3.getInt("level"));

    // an empty authority
    Document file = parse(stage, "file:///data/reports/q3.pdf");
    assertEquals("file", file.getString("scheme"));
    assertEquals("", file.getString("authority"));
    assertEquals("q3.pdf", file.getString("filename"));
    assertEquals("/data/reports", file.getString("folder"));
    assertEquals("/data/reports/q3.pdf", file.getString("path"));
    assertEquals(List.of("data", "data/reports"), file.getStringList("file_paths"));
    assertEquals(Integer.valueOf(3), file.getInt("level"));

    // Azure: the container is the first segment of the path
    Document azure = parse(stage, "https://acct.blob.core.windows.net/container/dir/file.txt");
    assertEquals("https", azure.getString("scheme"));
    assertEquals("acct.blob.core.windows.net", azure.getString("authority"));
    assertEquals("/container/dir", azure.getString("folder"));
    assertEquals("/container/dir/file.txt", azure.getString("path"));
    assertEquals(List.of("container", "container/dir"), azure.getStringList("file_paths"));
    assertEquals(Integer.valueOf(3), azure.getInt("level"));
  }

  @Test
  public void testParseUriEdgeCases() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/"));

    // the query and fragment are dropped, a port is kept in the authority, and the path is normalized
    Document query = parse(stage, "https://host:8080/a/b/../c/file.txt?x=1#frag");
    assertEquals("host:8080", query.getString("authority"));
    assertEquals("/a/c/file.txt", query.getString("path"));
    assertEquals(List.of("a", "a/c"), query.getStringList("file_paths"));

    // only a scheme and authority
    Document bucket = parse(stage, "s3://bucket");
    assertEquals("bucket", bucket.getString("authority"));
    assertEquals("", bucket.getString("path"));
    assertEquals("", bucket.getString("filename"));
    assertFalse(bucket.has("file_paths"));

    // java.net.URI rejects a raw space and a backslash, so the authority ends at the first separator
    Document space = parse(stage, "s3://bucket/my folder/file.txt");
    assertEquals("bucket", space.getString("authority"));
    assertEquals("/my folder/file.txt", space.getString("path"));
    Document backslash = parse(stage, "s3://bucket\\a\\file.txt");
    assertEquals("bucket", backslash.getString("authority"));
    assertEquals("/a/file.txt", backslash.getString("path"));

    // a path that leaves the root is parsed as a path
    Document escaping = parse(stage, "s3://bucket/../a/file.txt");
    assertFalse(escaping.has("scheme"));
    assertEquals("s3:/a/file.txt", escaping.getString("path"));
  }

  @Test
  public void testParseUriWindowsSeparator() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "\\"));
    Document doc = parse(stage, "s3://bucket/a/b/file.txt");
    assertEquals("bucket", doc.getString("authority"));
    assertEquals("\\a\\b\\file.txt", doc.getString("path"));
    assertEquals("\\a\\b", doc.getString("folder"));
    assertEquals(List.of("a", "a\\b"), doc.getStringList("file_paths"));
  }

  @Test
  public void testDriveAndUncPathsAreNotUris() throws StageException {
    Stage unix = factory.get(Map.of("fileSeparator", "/"));
    Document drive = parse(unix, "C://dir/file.txt");
    assertFalse(drive.has("scheme"));
    assertEquals("C:/dir/file.txt", drive.getString("path"));
    assertEquals(List.of("C:", "C:/dir"), drive.getStringList("file_paths"));

    Stage windows = factory.get(Map.of("fileSeparator", "\\"));
    Document unc = parse(windows, "\\\\server\\share\\dir\\file.txt");
    assertFalse(unc.has("scheme"));
    assertFalse(unc.has("authority"));
    assertEquals("\\\\server\\share\\dir\\file.txt", unc.getString("path"));
    assertEquals("\\\\server\\share\\dir", unc.getString("folder"));
    assertEquals(List.of("server", "server\\share", "server\\share\\dir"), unc.getStringList("file_paths"));
  }

  @Test
  public void testParseUrisDisabled() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/", "parseUris", false));
    Document doc = parse(stage, "s3://acme-docs/legal/2026/contract.docx");

    assertFalse(doc.has("scheme"));
    assertFalse(doc.has("authority"));
    assertEquals("s3:/acme-docs/legal/2026/contract.docx", doc.getString("path"));
    assertEquals("s3:/acme-docs/legal/2026", doc.getString("folder"));
    assertEquals(List.of("s3:", "s3:/acme-docs", "s3:/acme-docs/legal", "s3:/acme-docs/legal/2026"),
        doc.getStringList("file_paths"));
  }

  @Test
  public void testDecodeFilePath() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/", "decodeFilePath", true));

    // a URI is split before its authority and path are decoded, so an encoded "/" in the authority stays in it
    Document doc1 = parse(stage, "s3://my%2Fbucket/my%20folder/caf%C3%A9.txt");
    assertEquals("my/bucket", doc1.getString("authority"));
    assertEquals("/my folder/caf\u00e9.txt", doc1.getString("path"));
    assertEquals("caf\u00e9.txt", doc1.getString("filename"));
    assertEquals("/my folder", doc1.getString("folder"));
    assertEquals(List.of("my folder"), doc1.getStringList("file_paths"));

    // "+" is kept, unlike form decoding
    assertEquals("/dir/c++ notes.txt", parse(stage, "/dir/c++%20notes.txt").getString("path"));

    // an invalid escape sequence leaves the path undecoded
    Document invalid = parse(stage, "/dir/100%.txt");
    assertEquals("/dir/100%.txt", invalid.getString("path"));
    assertEquals("100%.txt", invalid.getString("filename"));

    // so does an encoded null character, which FilenameUtils would reject
    Document nul = parse(stage, "/data/a%00b%20c.txt");
    assertEquals("/data/a%00b%20c.txt", nul.getString("path"));
    assertEquals("a%00b%20c.txt", nul.getString("filename"));
    assertEquals("/data/a%00b.txt", parse(stage, "s3://bucket/data/a%00b.txt").getString("path"));

    // a drive letter is not a URI scheme
    assertEquals("C:/dir/a b.txt", parse(stage, "C://dir/a%20b.txt").getString("path"));
  }

  @Test
  public void testDecodeFilePathWindowsSeparator() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "\\", "decodeFilePath", true));
    Document doc = parse(stage, "C:%5Cmy%20folder%5Cfile.txt");

    assertEquals("C:\\my folder\\file.txt", doc.getString("path"));
    assertEquals("C:\\my folder", doc.getString("folder"));
    assertEquals(List.of("C:", "C:\\my folder"), doc.getStringList("file_paths"));
  }

  @Test
  public void testDecodeFilePathDisabledByDefault() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/"));
    Document doc = parse(stage, "s3://bucket/my%20folder/file.txt");

    assertEquals("/my%20folder/file.txt", doc.getString("path"));
    assertEquals("/my%20folder", doc.getString("folder"));
  }

  @Test
  public void testIncludeLevel() throws StageException {
    Stage unix = factory.get(Map.of("fileSeparator", "/", "includeLevel", true, "includeHierarchy", false));
    assertEquals(Integer.valueOf(3), parse(unix, "/a/b/file.txt").getInt("level"));
    assertEquals(Integer.valueOf(3), parse(unix, "a//b///file.txt/").getInt("level"));
    assertEquals(Integer.valueOf(1), parse(unix, "file.txt").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(unix, "/").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(unix, "").getInt("level"));
    assertEquals(Integer.valueOf(2), parse(unix, "s3://bucket/a/file.txt").getInt("level"));
    assertEquals(Integer.valueOf(0), parse(unix, "s3://bucket").getInt("level"));
    assertFalse(parse(unix, "/a/b/file.txt").has("file_paths"));

    Stage windows = factory.get(Map.of("fileSeparator", "\\", "includeLevel", true));
    assertEquals(Integer.valueOf(3), parse(windows, "C:\\a\\file.txt").getInt("level"));

    Stage noUris = factory.get(Map.of("fileSeparator", "/", "includeLevel", true, "parseUris", false));
    assertEquals(Integer.valueOf(4), parse(noUris, "s3://bucket/a/file.txt").getInt("level"));

    Stage defaults = factory.get(Map.of("fileSeparator", "/"));
    assertFalse(parse(defaults, "/a/b/file.txt").has("level"));
  }

  @Test
  public void testLevelMatchesHierarchy() throws StageException {
    for (String fileSep : List.of("/", "\\")) {
      Stage stage = factory.get(Map.of("fileSeparator", fileSep, "includeLevel", true, "uppercaseExtension", false));
      for (String path : levelTestPaths()) {
        if (FilenameUtils.normalizeNoEndSeparator(path, fileSep.equals("/")) == null) {
          continue;
        }
        Document doc = parse(stage, path);
        int hierarchySize = doc.has("file_paths") ? doc.getStringList("file_paths").size() : 0;
        int level = doc.getInt("level");
        assertEquals("fileSeparator=" + fileSep + ", path=" + path, Math.max(level - 1, 0), hierarchySize);
      }
    }
  }

  @Test
  public void testClearInvalidExtension() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/", "clearInvalidExtension", true));

    for (char ch : "\u2215\u2044\u29F8\uFF0F\uFE68\u2216\u29F9\uFF3C\n\t\u0001\u007F\u009F".toCharArray()) {
      Document doc = parse(stage, "/dir/file.a" + ch + "b");
      assertEquals("U+" + Integer.toHexString(ch), "", doc.getString("file_extension"));
      assertEquals("file.a" + ch + "b", doc.getString("filename"));
    }

    assertEquals("TXT", parse(stage, "/dir/file.txt").getString("file_extension"));
    assertEquals("", parse(stage, "/dir/no_extension").getString("file_extension"));
    assertEquals("\u00C9T\u00C9", parse(stage, "/dir/file.\u00E9t\u00E9").getString("file_extension"));

    Stage lowercase = factory.get(Map.of("fileSeparator", "/", "clearInvalidExtension", true, "uppercaseExtension", false));
    assertEquals("", parse(lowercase, "/dir/file.a\uFF0Fb").getString("file_extension"));
    assertEquals("txt", parse(lowercase, "/dir/file.txt").getString("file_extension"));
  }

  @Test
  public void testInvalidExtensionKeptByDefault() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/"));
    assertEquals("A\u2215B", parse(stage, "/dir/file.a\u2215b").getString("file_extension"));
  }

  @Test
  public void testRootedHierarchy() throws StageException {
    Stage unix = factory.get(Map.of("fileSeparator", "/", "rootedHierarchy", true));
    assertEquals(List.of("/a", "/a/b"), parse(unix, "/a/b/file.txt").getStringList("file_paths"));
    assertEquals(List.of("/a", "/a/b"), parse(unix, "a//b/file.txt").getStringList("file_paths"));
    assertFalse(parse(unix, "/file.txt").has("file_paths"));

    // only file_paths is rooted
    Document doc = parse(unix, "a/b/file.txt");
    assertEquals("a/b/file.txt", doc.getString("path"));
    assertEquals("a/b", doc.getString("folder"));

    Stage windows = factory.get(Map.of("fileSeparator", "\\", "rootedHierarchy", true));
    assertEquals(List.of("\\C:", "\\C:\\a"), parse(windows, "C:\\a\\file.txt").getStringList("file_paths"));
    assertEquals(List.of("/a", "/a/b"), parse(unix, "s3://bucket/a/b/file.txt").getStringList("file_paths"));
  }

  private static Document parse(Stage stage, String filePath) throws StageException {
    Document doc = Document.create("doc");
    doc.setField("file_path", filePath);
    stage.processDocument(doc);
    return doc;
  }

  // fixed, deep and seeded random paths, to check that level and file_paths always agree
  private static List<String> levelTestPaths() {
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

  // TODO: This is a benchmark rather than a unit test. It should eventually be moved to a benchmarking suite, along with
  // legacyProcessDocument above, which is only kept as the baseline it compares against.
  @Test
  public void testDeepPathBenchmark() throws StageException {
    Stage stage = factory.get(Map.of("fileSeparator", "/"));
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
