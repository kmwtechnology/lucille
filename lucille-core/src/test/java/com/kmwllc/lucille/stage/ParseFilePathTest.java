package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import java.io.File;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
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
  public void sandbox() throws Exception {
    String sentence = "Sheryl Sandberg spoke at a leadership conference hosted by Meta in Menlo Park.";

    // Tokenization of the sentence comes first
    InputStream tokenModelIn = ParseFilePathTest.class.getResourceAsStream("/en-token.bin");
    TokenizerModel tokenModel = new TokenizerModel(tokenModelIn);
    TokenizerME tokenizer = new TokenizerME(tokenModel);
    String[] tokens = tokenizer.tokenize(sentence);

    Map<String, NameFinderME> finders = new HashMap<>();
    finders.put("PERSON", new NameFinderME(new TokenNameFinderModel(ParseFilePathTest.class.getResourceAsStream("/en-ner-person.bin"))));
    finders.put("LOCATION", new NameFinderME(new TokenNameFinderModel(ParseFilePathTest.class.getResourceAsStream("/en-ner-location.bin"))));
    finders.put("ORGANIZATION", new NameFinderME(new TokenNameFinderModel(ParseFilePathTest.class.getResourceAsStream("/en-ner-organization.bin"))));

    // Each model is run on the sentence and we print out our findings
    for (Map.Entry<String, NameFinderME> entry : finders.entrySet()) {
      String type = entry.getKey();
      NameFinderME finder = entry.getValue();

      Span[] spans = finder.find(tokens);
      for (Span span : spans) {
        StringBuilder entity = new StringBuilder();
        for (int i = span.getStart(); i < span.getEnd(); i++) {
          entity.append(tokens[i]).append(" ");
        }
        System.out.println(type + ": " + entity.toString().trim());
      }
    }
  }

  @Test
  public void stanfordSandbox() throws Exception {
    String text = "Benjamin was in New York last week for the CME's annual advisor conference.";

    // setting up the models used
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");

    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
    Annotation document = new Annotation(text);
    pipeline.annotate(document);

    // Extracts sentences and then we run NER on them all
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
    for (CoreMap sentence : sentences) {
      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        // Retrieve and print the word and its NER label
        String word = token.word();
        String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
        // 'O' means no entity
        if (!"O".equals(ne)) {
          System.out.println(word + " : " + ne);
        }
      }
    }
  }
}
