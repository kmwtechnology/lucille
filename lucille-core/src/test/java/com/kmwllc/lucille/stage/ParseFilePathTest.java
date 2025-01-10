package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ParseFilePathTest {

  private final StageFactory factory = StageFactory.of(ParseFilePath.class);

  @Test
  public void testFilePathParse() throws StageException {
    
    Stage stage = factory.get("ParseFilePathTest/conf.conf");
    Document doc1 = Document.create("doc1");
    //doc1.setField("file_path", "Z:\\2015-11-14\\aie30\\version3.0\\app\\test\\com\\attivio\\app\\config\\feature\\content-store-feature-memory.xml");
    doc1.setField("file_path", "Z:\\folder1\\folder2\\folder3\\myfile.xml");
    stage.processDocument(doc1);
    
    // System.err.println(doc1);
    
    assertTrue(doc1.has("filename"));
    assertEquals("myfile.xml", doc1.getString("filename"));
    
    assertTrue(doc1.has("file_extension"));
    assertEquals("XML", doc1.getString("file_extension"));

    assertTrue(doc1.has("folder"));
    assertEquals("Z:\\folder1\\folder2\\folder3", doc1.getString("folder"));
    
    assertTrue(doc1.has("file_paths"));
    assertEquals(4, doc1.getStringList("file_paths").size());
    
    assertEquals("Z:", doc1.getStringList("file_paths").get(0));
    assertEquals("Z:\\folder1", doc1.getStringList("file_paths").get(1));
    assertEquals("Z:\\folder1\\folder2", doc1.getStringList("file_paths").get(2));
    assertEquals("Z:\\folder1\\folder2\\folder3", doc1.getStringList("file_paths").get(3));
    
  }

}