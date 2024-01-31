package com.kmwllc.lucille.stage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jsoup.Jsoup;
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
 * <b>filePathField</b> (String, Optional): field name which contains a path to an HTML document which will be processed
 * </p>
 * <p>
 * <b>byteArrayField</b> (String, Optional): field name which contains a byte array of the html which will be processed (encoded in UTF-8).
 * byteArrayField and filePathField cannot both be specified. If a document does not have whichever field is specified in the config, then it is 
 * not modified
 * </p>
 * <b>destinationFields</b> (Map<String, String>) : defines a mapping from css selectors to the destination fields 
 * in the processed document. If they already exist they are overwritten. Otherwise, they are created. If a selector returns 
 * multiple elements the destination field receives a list of processed elements. If a selector returns no elements for a document then the 
 * destination field is not created
 * @see <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax"> CSS selectors </a> for information on supported selectors
 * </p>
 */
public class JSoupExtractor extends Stage {

  private String filePathField;
  private Map<String, Object> destinationFields;
  private String byteArrayField;

  public JSoupExtractor(Config config) {
    super(config,
        new StageSpec().withOptionalProperties("filePathField", "byteArrayField").withRequiredParents("destinationFields"));

    this.destinationFields = config.getConfig("destinationFields").root().unwrapped();
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", null);
    this.byteArrayField = ConfigUtils.getOrDefault(config, "byteArrayField", null);
  }

  @Override
  public void start() throws StageException {
    if (this.filePathField != null && this.byteArrayField != null) {
      throw new StageException("Stage cannot have both filePathField and byteArrayField specified");
    }

    for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        Map<String, String> map = (Map<String, String>) value;
        if (map.keySet().size() != 2)
          throw new StageException("map must contain two fields");
        if (!map.containsKey("attribute") || !map.containsKey("selector"))
          throw new StageException("map must contain a attribute and selector fields");
      } else if (!(value instanceof String)) {
        throw new StageException("destinationFields mapping can only have strings or a map as its values");
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String field = filePathField != null ? filePathField : byteArrayField;

    if (doc.has(field)) {
      try {
        org.jsoup.nodes.Document jsoupDoc = filePathField != null ? Jsoup.parse(new File(doc.getString(filePathField)))
            : Jsoup.parse(new String(doc.getBytes(byteArrayField), StandardCharsets.UTF_8));
        System.out.println(super.getName() + " " + destinationFields.entrySet());
        for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
          Object value = entry.getValue();

          if (value instanceof String) {
            // if the user specified a `desination -> selector` mapping
            doc.update(entry.getKey(), UpdateMode.OVERWRITE, jsoupDoc.select((String) value).eachText().toArray(new String[0]));
          } else {
            // if the user specified a `destination -> {selector: selector, attribute: attribute}` mapping
            Map<String, String> config = (Map<String, String>) value;
            doc.update(entry.getKey(), UpdateMode.OVERWRITE, jsoupDoc.select(config.get("selector")).eachAttr(config.get("attribute")).toArray(new String[0]));
          }

        }
      } catch (IOException e) {
        throw new StageException("File parse failed: " + e.getMessage());
      }
    }

    return null;
  }
}
