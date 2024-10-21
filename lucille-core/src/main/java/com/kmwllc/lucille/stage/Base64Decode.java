package com.kmwllc.lucille.stage;

import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

public class Base64Decode extends Stage {

  private String inputField;
  private String outputField;
  
  public Base64Decode(Config config) {
    super(config, new StageSpec().withRequiredProperties("inputField", "outputField"));
    inputField = config.getString("inputField");
    outputField = config.getString("outputField");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (doc.has(inputField)) {
      String base64Data = doc.getString(inputField);
      byte[] decodedData = Base64.decodeBase64(base64Data);
      doc.setField(outputField, decodedData);
    }
    return null;
  }

}
