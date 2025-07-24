package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * A Stage for decoding base64 data strings and outputting them as arrays of bytes on a document.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>input_field (String) : The field containing base64 data Strings you want to decode.</li>
 *   <li>output_field (String) : The field you want to place the decoded data (arrays of bytes) into.</li>
 * </ul>
 */
public class Base64Decode extends Stage {

  private String inputField;
  private String outputField;

  public Base64Decode(Config config) {
    super(config, Spec.stage().withRequiredProperties("input_field", "output_field"));
    inputField = config.getString("input_field");
    outputField = config.getString("output_field");
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
