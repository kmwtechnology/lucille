package com.kmwllc.lucille.stage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import org.jsoup.Jsoup;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

/**
 * Selects elements from an HTML document and extracts the raw text from them into destination fields
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>filePathField</b> (String, Optional) : field name which contains a path to an html document which will be processed
 * </p>
 * <p>
 * <b>stringField</b> (String, Optional): field name which contains a string of the html which will be processed.
 * </p>
 * <p>
 * <b>byteArrayField</b> (String, Optional) : field name which contains a byte array of the html which will be processed.
 * only one of fieldPathField, stringField, or byteArrayField can be specified. If a document does not have whichever field is specified in the config, 
 * then it is not modified
 * </p>
 * <p>
 * <b>charset</b> (String, Optional) : the encoding of the html document. If none is provided when filePathField is provided the charset is detected from 
 * the byte-order-mark (BOM) or meta tags and defaults to UTF-8 if none is found. If no charset is provided when byteArrayField is provided the charset defaults 
 * to UTF-8 immediately. Has no effect if stringField is provided.
 * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html"> Charsets </a> for information on supported charsets and conventions for 
 * specifying them.
 * </p>
 * <b>destinationFields</b> (Map<String, String>) : defines a mapping from destination fields to css selectors.
 * If a destination field already exists in the processed document it is overwritten. Otherwise, they are created. If a selector returns 
 * multiple elements the destination field receives a list of processed elements. If a selector returns no elements for a document then the 
 * destination field is not created
 * @see <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax"> CSS selectors </a> for information on supported selectors
 * </p>
 */
public class ApplyJSoup extends Stage {

  private final String filePathField;
  private final Map<String, Object> destinationFields;
  private final String byteArrayField;
  private final String stringField;
  private final String charset;

  public ApplyJSoup(Config config) {
    super(config, new StageSpec().withOptionalProperties("filePathField", "byteArrayField", "stringField", "charset")
        .withRequiredParents("destinationFields"));

    this.destinationFields = config.getConfig("destinationFields").root().unwrapped();
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", null);
    this.byteArrayField = ConfigUtils.getOrDefault(config, "byteArrayField", null);
    this.charset = ConfigUtils.getOrDefault(config, "charset", null);
    this.stringField = ConfigUtils.getOrDefault(config, "stringField", null);
  }

  @Override
  public void start() throws StageException {
    if (!((this.filePathField != null ^ this.byteArrayField != null ^ this.stringField != null
        ^ (this.filePathField != null && this.byteArrayField != null && this.stringField != null)))) {
      throw new StageException("Stage must have one and only one of filePathField, stringField, or byteArrayField specified");
    }

    for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        Map<String, String> map = (Map<String, String>) value;
        if (map.keySet().size() != 2) {
          throw new StageException("map must contain two fields");
        }
        if (!map.containsKey("attribute") || !map.containsKey("selector")) {
          throw new StageException("map must contain a attribute and selector fields");
        }
      } else if (!(value instanceof String)) {
        throw new StageException("destinationFields mapping can only have strings or a map as its values");
      }
    }
  }

  private String activeField() {
    if (filePathField != null) {
      return filePathField;
    } else if (byteArrayField != null) {
      return byteArrayField;
    } else {
      return stringField;
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String field = activeField();
    if (!doc.has(field)) {
      return null;
    }

    org.jsoup.nodes.Document jsoupDoc;
    if (filePathField != null) {
      try {
        jsoupDoc = Jsoup.parse(new File(doc.getString(filePathField)), charset);
      } catch (IOException e) {
        // only the File creation can throw an IOException
        throw new StageException("File parse failed: " + e.getMessage());
      }
    } else if (byteArrayField != null) {
      jsoupDoc = Jsoup
          .parse(new String(doc.getBytes(byteArrayField), charset != null ? Charset.forName(charset) : StandardCharsets.UTF_8));
    } else {
      jsoupDoc = Jsoup.parse(doc.getString(stringField));
    }

    for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
      Object value = entry.getValue();

      if (value instanceof String) {
        // if the user specified a `destination -> selector` mapping
        doc.update(entry.getKey(), UpdateMode.OVERWRITE, jsoupDoc.select((String) value).eachText().toArray(new String[0]));
      } else {
        // if the user specified a `destination -> {selector: selector, attribute: attribute}` mapping
        Map<String, String> config = (Map<String, String>) value;
        doc.update(entry.getKey(), UpdateMode.OVERWRITE,
            jsoupDoc.select(config.get("selector")).eachAttr(config.get("attribute")).toArray(new String[0]));
      }
    }

    return null;
  }
}
