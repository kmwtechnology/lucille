package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class EntityRecognitionTest {

  private final StageFactory factory = StageFactory.of(EntityRecognition.class);

  @Test
  public void sandbox() throws StageException {
    Config config = ConfigFactory.parseMap(Map.of("textField", "text"));
    Stage stage = factory.get(config);

    Document doc = Document.create("doc1");
    doc.setField("text", "Sheryl Sandberg spoke at a leadership conference hosted by the NYSE in Menlo Park.");

    stage.processDocument(doc);
    System.out.println(doc);
//    System.out.println(doc);
  }
}
