package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;

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
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>filePathField (String, Optional) : The field name that contains the file path. Defaults to "file_path".</li>
 *   <li>fileSep (String, Optional) : The separator to use in the normalized path output from this stage. Defaults to the operating
 *   system's separator. <b>Must</b> be either "/" or "\". You should specify the separator if you are running Lucille on Windows but
 *   working with Unix file paths (or vice versa) and you want the original separators to be preserved. Note that this stage will
 *   parse Windows and Unix paths regardless of fileSep / operating system - this property only controls output behavior.</li>
 *   <li>uppercaseExtension (Boolean, Optional) : If true, the extracted file extension will be in all uppercase letters. Defaults to
 *   true.</li>
 *   <li>includeHierarchy (Boolean, Optional) : If true, a field ("file_paths") will be populated with all of the normalized subpaths
 *   leading up to the full file path (to aid in building a hierarchical aggregator/facet for a search engine).</li>
 * </ul>
 */
public class ParseFilePath extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("filePathField", "fileSep")
      .optionalBoolean("uppercaseExtension", "includeHierarchy").build();

  private final String filePathField;
  private final String fileSep;
  private final boolean uppercaseExtension;
  private final boolean includeHierarchy;
  private final boolean useUnix;
  private final char sepChar;

  private static final Logger log = LoggerFactory.getLogger(ParseFilePath.class);
  
  public ParseFilePath(Config config) {
    super(config);
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileSep = ConfigUtils.getOrDefault(config, "fileSep", File.separator);

    if (!fileSep.equals("/") && !fileSep.equals("\\")) {
      throw new IllegalArgumentException("ParseFilePath must have fileSep set to either / or \\. Remember to escape \\ in your .conf.");
    }

    this.uppercaseExtension = ConfigUtils.getOrDefault(config, "uppercaseExtension", true);
    this.includeHierarchy = ConfigUtils.getOrDefault(config, "includeHierarchy", true);
    this.useUnix = fileSep.equals("/");
    this.sepChar = fileSep.charAt(0);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = FilenameUtils.normalizeNoEndSeparator(doc.getString(filePathField), useUnix);

    doc.addToField("filename", FilenameUtils.getName(filePath));
    doc.addToField("folder", FilenameUtils.getFullPathNoEndSeparator(filePath));
    doc.addToField("path", filePath);

    if (uppercaseExtension) {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath).toUpperCase());
    } else {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath));
    }

    if (includeHierarchy) {
      addHierarchy(doc, filePath);
    }

    return null;
  }

  /**
   * Adds each proper prefix of the path's segments to "file_paths". Segments are split the same way as
   * StringUtils.split(path, fileSep): runs of separators never yield an empty segment, and leading or trailing
   * separators are dropped. The prefixes are built in one growing StringBuilder, with one append per segment,
   * instead of re-joining all of the earlier segments for every prefix.
   */
  private void addHierarchy(Document doc, String path) {
    StringBuilder prefix = new StringBuilder(path.length());
    int start = skipSeparators(path, 0);
    int emitted = 0;

    while (start < path.length()) {
      int end = path.indexOf(sepChar, start);
      int next = end < 0 ? path.length() : skipSeparators(path, end);
      // the last segment is the full path, which is not part of the hierarchy
      if (next == path.length()) {
        break;
      }

      if (emitted > 0) {
        prefix.append(sepChar);
      }
      prefix.append(path, start, end);
      doc.addToField("file_paths", prefix.toString());
      emitted++;
      start = next;
    }
  }

  private int skipSeparators(String path, int pos) {
    while (pos < path.length() && path.charAt(pos) == sepChar) {
      pos++;
    }
    return pos;
  }
}

