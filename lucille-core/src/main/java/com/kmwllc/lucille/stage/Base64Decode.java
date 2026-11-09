package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
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
 *   <li>source (String) : The field containing base64 data Strings you want to decode.</li>
 *   <li>dest (String) : The field you want to place the decoded data (arrays of bytes) into.</li>
 * </ul>
 */
public class Base64Decode extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source", "dest").build();

  private final String inputField;
  private final String outputField;

  public Base64Decode(Config config) {
    super(config);
    inputField = config.getString("source");
    outputField = config.getString("dest");
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
