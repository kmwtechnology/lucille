package com.kmwllc.lucille.stage;

import java.io.File;
import java.io.IOException;
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
 * <b>filePathField</b> (String): field name which contains a path to an HTML document which will be processed
 * </p>
 * <b>destinationFields</b> (Map<String, String>) : defines a mapping from css selectors to the destination fields 
 * in the processed document. If they already exist they are overwritten. Otherwise, they are created. If a selector returns 
 * multiple elements the destination field receives a list of processed elements. 
 * @see <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax"> CSS selectors </a> for information on supported selectors.
 * </p>
 */
public class HTMLTextExtract extends Stage {

  private String filePathField;
  private Map<String, Object> destinationFields;

  public HTMLTextExtract(Config config) {
    super(config, new StageSpec().withRequiredProperties("filePathField").withRequiredParents("destinationFields"));

    this.destinationFields = config.getConfig("destinationFields").root().unwrapped();
    this.filePathField = config.getString("filePathField");
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", null);
  }


  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (doc.has(filePathField)) {
      File file = new File(doc.getString(filePathField));

      try {
        org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(file);
        for (Map.Entry<String, Object> entry : destinationFields.entrySet()) {
          doc.update(entry.getKey(), UpdateMode.OVERWRITE,
              jsoupDoc.select((String) entry.getValue()).eachText().toArray(new String[0]));
        }
      } catch (IOException e) {
        throw new StageException("File parse failed: " + e.getMessage());
      }
    }

    return null;
  }
}
