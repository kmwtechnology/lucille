package com.kmwllc.lucille.stage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.jsoup.Jsoup;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;


public class HTMLTextExtract extends Stage {

  private List<String> fieldNames;
  private List<String> elements;
  private String filePathField;

  public HTMLTextExtract(Config config) {
    super(config, new StageSpec().withOptionalProperties("file_path_field", "text_fields", "elements"));

    this.fieldNames = ConfigUtils.getOrDefault(config, "text_fields", null);
    this.elements = ConfigUtils.getOrDefault(config, "elements", null);
    this.filePathField = ConfigUtils.getOrDefault(config, "file_path_field", null);
  }

  @Override
  public void start() throws StageException {
    if(elements.size() != fieldNames.size()) {
      throw new StageException("Number of elements and text fields must be equal");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    
    if(doc.has(filePathField)) {
      File file = new File(doc.getString(filePathField));

      try {
        org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(file, "UTF-8");
        int i = 0;
        for(String element : elements) {
          doc.update(fieldNames.get(i++), UpdateMode.OVERWRITE, jsoupDoc.select(element).text());
        }
      } catch(IOException e) {
        throw new StageException("File parse failed: " + e.getMessage());
      }
    }

    return null;
  }

}
