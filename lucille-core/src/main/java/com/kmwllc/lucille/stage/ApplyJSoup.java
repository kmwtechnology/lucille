package com.kmwllc.lucille.stage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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
 * <b>destinationFields</b> (Map&lt;String, Map&lt;String, String&gt;&gt;) : defines a mapping from destination fields to selector maps. Selector maps have a `selector` field which takes 
 * a css selector and a `type` field which takes one of the following: 'text', 'attribute', 'html', or 'outerHtml'. When doing attribute extraction an additional `attribute` with 
 * the desired attribute must be specified. For example:
 * <p>
 *  destinationFields: {
 *    destination1: {
 *      type: "attribute",
 *      selector: ".foo",
 *      attribute: "href"
 *    } 
 *  }
 * </p>
 * If a destination field already exists in the processed document it is overwritten. Otherwise, they are created. If a selector returns 
 * multiple elements the destination field receives a list of processed elements. If a selector returns no elements for a document then the 
 * destination field is not created.  
 * @see <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax"> CSS selectors </a> for information on supported selectors
 * </p>
 */
public class ApplyJSoup extends Stage {

  private final Map<String, Object> destinationFields;
  private final String charset;
  private final String activeField;
  private final Mode mode;

  private enum Mode {
    STRING, BYTE, FILE
  }

  public ApplyJSoup(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("filePathField", "byteArrayField", "stringField", "charset")
        .withRequiredParents("destinationFields"));

    this.destinationFields = config.getConfig("destinationFields").root().unwrapped();
    this.charset = ConfigUtils.getOrDefault(config, "charset", null);

    if (!((config.hasPath("filePathField") ^ config.hasPath("byteArrayField") ^ config.hasPath("stringField")
        ^ (config.hasPath("filePathField") && config.hasPath("byteArrayField") && config.hasPath("stringField"))))) {
      throw new StageException("Stage must have one and only one of filePathField, stringField, or byteArrayField specified");
    }

    if (config.hasPath("filePathField")) {
      activeField = config.getString("filePathField");
      mode = Mode.FILE;
    } else if (config.hasPath("byteArrayField")) {
      activeField = config.getString("byteArrayField");
      mode = Mode.BYTE;
    } else {
      activeField = config.getString("stringField");
      mode = Mode.STRING;
    }
  }

  @Override
  public void start() throws StageException {

    for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
      Object value = entry.getValue();
      if (!(value instanceof Map)) {
        throw new StageException("destination fields must be mapped to a selector map");
      }
      Map<String, String> map = (Map<String, String>) value;
      if (!map.containsKey("type")) {
        throw new StageException("selector map must contain a `type` field");
      }
      if (!map.containsKey("selector")) {
        throw new StageException("selector map must contain a `selector` field");
      }
      if (map.get("type").equals("attribute") && !map.containsKey("attribute")) {
        throw new StageException("`attribute` field must be provided when doing attribute extraction");
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(activeField)) {
      return null;
    }

    org.jsoup.nodes.Document jsoupDoc = null;
    switch (mode) {
      case FILE: {
        try {
          jsoupDoc = Jsoup.parse(new File(doc.getString(activeField)), charset);
        } catch (IOException e) {
          // only the File creation can throw an IOException
          throw new StageException("File parse failed: " + e.getMessage());
        }
        break;
      }
      case BYTE: {
        jsoupDoc =
            Jsoup.parse(new String(doc.getBytes(activeField), charset != null ? Charset.forName(charset) : StandardCharsets.UTF_8));
        break;
      }
      case STRING: {
        jsoupDoc = Jsoup.parse(doc.getString(activeField));
        break;
      }
    }

    for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
      Map<String, String> value = (Map<String, String>) entry.getValue();
      Elements selected = jsoupDoc.select(value.get("selector"));
      List<String> extractedText = new ArrayList<>();

      switch (value.get("type")) {
        case "text": {
          extractedText = selected.eachText();
          break;
        }
        case "attribute": {
          extractedText = selected.eachAttr(value.get("attribute"));
          break;
        }
        case "html": {
          for (Element e : selected) {
            extractedText.add(e.html());
          }
          break;
        }
        case "outerHtml": {
          for (Element e : selected) {
            extractedText.add(e.outerHtml());
          }
          break;
        }
        default:
          break;
      }

      doc.update(entry.getKey(), UpdateMode.OVERWRITE, extractedText.toArray(new String[0]));
    }

    return null;
  }
}
