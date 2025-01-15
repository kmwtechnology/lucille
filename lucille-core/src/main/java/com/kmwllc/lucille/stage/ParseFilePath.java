package com.kmwllc.lucille.stage;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import opennlp.tools.stemmer.snowball.finnishStemmer;

/**
 * This stage will take a field that contains a file path on it like  c:\directory\filename.txt and parse out the filename, 
 * file extension, folder and it will also create a heirarchical representation of the folder.
 *
 * Config Parameters:
 *
 *   - file_path_field (String) - the field name that contains the file path
 *   - file_sep (String) - the file system separator defaults to the operating system separator
 *   - uppercase_extension (Boolean) - if this is true the extracted file extension will be upper case
 *   - include_heirarchy (Boolean) - if this is true a field will be populated with all of the subpaths 
 *   in the directory structure so a heirarchical aggregator/facet can be generated in a search engine with it.
 *   
 */
public class ParseFilePath extends Stage {

  private final String filePathField;
  private final String fileSep;
  private final boolean uppercaseExtension;
  private final boolean includeHeirarchy;
  private static final Logger log = LogManager.getLogger(ParseFilePath.class);
  
  public ParseFilePath(Config config) {
    super(config, new StageSpec().withOptionalProperties("file_path_field", "file_sep", "uppercase_extension","include_heirarchy"));
    this.filePathField = config.hasPath("file_path_field") ? config.getString("file_path_field") : "file_path";
    this.fileSep = config.hasPath("file_sep") ? config.getString("file_sep") : ""+File.separatorChar;
    this.uppercaseExtension = config.hasPath("uppercase_extension") ? config.getBoolean("uppercase_extension") : true;
    this.includeHeirarchy = config.hasPath("include_heirarchy") ? config.getBoolean("include_heirarchy") : true;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (doc.has(filePathField)) {
      String filePath = doc.getString(filePathField);
      
      File f = new File(filePath);
      doc.addToField("filename" ,f.getName());
      doc.addToField("folder" ,f.getParent());
      doc.addToField("path" ,f.getPath());
      if (uppercaseExtension) {
        doc.addToField("file_extension", FilenameUtils.getExtension(f.getName()).toUpperCase());
      } else {
        doc.addToField("file_extension", FilenameUtils.getExtension(f.getName()));
      }
      if (includeHeirarchy) {
        String[] paths = StringUtils.split(f.getPath(), fileSep);
        for (int i = 1; i < paths.length; i++) {
          String subPath = StringUtils.join(Arrays.copyOfRange(paths, 0, i), fileSep);
          doc.addToField("file_paths", subPath);
        }
      }
    }

    return null;

  }

}

