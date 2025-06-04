package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage will take a field that contains a file path (like  c:\directory\filename.txt) and parse out information about the
 * path:
 *   <br> - filename: The name of the file.
 *   <br> - folder: The parent of the file.
 *   <br> - path: The path to the file. (Normalized)
 *   <br> - file_extension: The extension of the file.
 *
 * <br> <br> <b>Config Parameters:</b>
 *
 *   <br> - filePathField (String, Optional) - The field name that contains the file path. Defaults to "file_path".
 *   <br> - fileSep (String, Optional) - The separator to use in the normalized path output from this stage.
 *   Defaults to the operating system's separator. <b>Must</b> be either "/" or "\". You should specify the separator if you are
 *   running Lucille on Windows but working with Unix file paths (or vice versa) and you want the original separators to be
 *   preserved. Note that this stage will parse Windows and Unix paths regardless of fileSep / operating system - this property
 *   only controls output behavior.
 *   <br> - uppercaseExtension (Boolean, Optional) - If true, the extracted file extension will be in all uppercase letters. Defaults
 *   to true.
 *   <br> - includeHierarchy (Boolean, Optional) - If true, a field ("file_paths") will be populated with all of the normalized
 *   subpaths leading up to the full file path (to aid in building a hierarchical aggregator/facet for a search engine).
 *   
 */
public class ParseFilePath extends Stage {

  private final String filePathField;
  private final String fileSep;
  private final boolean uppercaseExtension;
  private final boolean includeHierarchy;

  private static final Logger log = LoggerFactory.getLogger(ParseFilePath.class);
  
  public ParseFilePath(Config config) {
    super(config, Spec.stage().withOptionalProperties("filePathField", "fileSep", "uppercaseExtension", "includeHierarchy"));
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileSep = ConfigUtils.getOrDefault(config, "fileSep", File.separator);

    if (!fileSep.equals("/") && !fileSep.equals("\\")) {
      throw new IllegalArgumentException("ParseFilePath must have fileSep set to either / or \\. Remember to escape \\ in your .conf.");
    }

    this.uppercaseExtension = ConfigUtils.getOrDefault(config, "uppercaseExtension", true);
    this.includeHierarchy = ConfigUtils.getOrDefault(config, "includeHierarchy", true);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);
    boolean useUnix = fileSep.equals("/");
    filePath = FilenameUtils.normalizeNoEndSeparator(filePath, useUnix);

    doc.addToField("filename", FilenameUtils.getName(filePath));
    doc.addToField("folder", FilenameUtils.getFullPathNoEndSeparator(filePath));
    doc.addToField("path", filePath);

    if (uppercaseExtension) {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath).toUpperCase());
    } else {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath));
    }

    if (includeHierarchy) {
      String[] paths = StringUtils.split(FilenameUtils.normalizeNoEndSeparator(filePath, useUnix), fileSep);
      for (int i = 1; i < paths.length; i++) {
        String subPath = StringUtils.join(Arrays.copyOfRange(paths, 0, i), fileSep);
        doc.addToField("file_paths", subPath);
      }
    }

    return null;
  }
}

