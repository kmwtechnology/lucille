package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.spec.NumberProperty;
import com.kmwllc.lucille.core.spec.Property;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

import java.util.Map;
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
  public void configSandbox() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("ParseFilePathTest/sandbox.conf");

    Property requiredNumberProperty = new NumberProperty("field", true);
    Property optionalNumberProperty = new NumberProperty("field", false);

    requiredNumberProperty.validate(config);
    optionalNumberProperty.validate(config);
  }
}
