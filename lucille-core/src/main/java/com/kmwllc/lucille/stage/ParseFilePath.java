package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 *   <li>decodeFilePath (Boolean, Optional) : If true, the file path is URL-decoded (UTF-8) before it is parsed, and the "://" after a
 *   URI scheme (like s3://bucket/key) is kept in the normalized path instead of being collapsed to ":/". A "+" is kept as is, and a
 *   path with an invalid escape sequence is parsed without decoding. Defaults to false.</li>
 *   <li>createRelativeHierarchy (Boolean, Optional) : If true, the "file_paths" of a URI are built from its path without the scheme
 *   and authority, so s3://bucket/a/b/file.txt gives "a" and "a/b" instead of "s3:", "s3:/bucket", "s3:/bucket/a" and
 *   "s3:/bucket/a/b". Other paths are not affected. Defaults to false.</li>
 *   <li>includeLevel (Boolean, Optional) : If true, a field ("level") will be populated with the number of segments in the path,
 *   including the file name: 3 for /a/b/file.txt and for C:\a\file.txt. Segments are counted the same way as for "file_paths", so
 *   createRelativeHierarchy applies to the level too. Defaults to false.</li>
 * </ul>
 */
public class ParseFilePath extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("filePathField", "fileSep")
      .optionalBoolean("uppercaseExtension", "includeHierarchy", "decodeFilePath", "createRelativeHierarchy", "includeLevel")
      .build();

  // A URI scheme followed by "://". Schemes of one letter are not matched, so that a Windows drive (C://dir) is not taken for one.
  private static final Pattern URI_SCHEME = Pattern.compile("[a-zA-Z][a-zA-Z0-9+.-]+://");

  private final String filePathField;
  private final String fileSep;
  private final boolean uppercaseExtension;
  private final boolean includeHierarchy;
  private final boolean decodeFilePath;
  private final boolean createRelativeHierarchy;
  private final boolean includeLevel;
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
    this.decodeFilePath = ConfigUtils.getOrDefault(config, "decodeFilePath", false);
    this.createRelativeHierarchy = ConfigUtils.getOrDefault(config, "createRelativeHierarchy", false);
    this.includeLevel = ConfigUtils.getOrDefault(config, "includeLevel", false);
    this.useUnix = fileSep.equals("/");
    this.sepChar = fileSep.charAt(0);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String rawPath = doc.getString(filePathField);
    String filePath;
    if (decodeFilePath) {
      rawPath = decode(rawPath);
      filePath = normalizeUri(rawPath);
    } else {
      filePath = FilenameUtils.normalizeNoEndSeparator(rawPath, useUnix);
    }

    doc.addToField("filename", FilenameUtils.getName(filePath));
    doc.addToField("folder", FilenameUtils.getFullPathNoEndSeparator(filePath));
    doc.addToField("path", filePath);

    if (uppercaseExtension) {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath).toUpperCase());
    } else {
      doc.addToField("file_extension", FilenameUtils.getExtension(filePath));
    }

    String hierarchyPath = createRelativeHierarchy ? relativeHierarchyPath(rawPath, filePath) : filePath;

    if (includeLevel) {
      doc.addToField("level", countSegments(hierarchyPath));
    }

    if (includeHierarchy) {
      addHierarchy(doc, hierarchyPath);
    }

    return null;
  }

  private static String decode(String filePath) {
    try {
      // URLDecoder decodes "+" to a space, which is only correct for form data. Encode it so it is kept in the path.
      return URLDecoder.decode(filePath.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      log.warn("Could not URL-decode file path {}, parsing it without decoding: {}", filePath, e.getMessage());
      return filePath;
    }
  }

  /**
   * Normalizes the path. If it starts with a URI scheme, only the part after the "://" is normalized, so the "://" is not
   * collapsed to ":/".
   */
  private String normalizeUri(String filePath) {
    Matcher scheme = URI_SCHEME.matcher(filePath);
    if (!scheme.lookingAt()) {
      return FilenameUtils.normalizeNoEndSeparator(filePath, useUnix);
    }

    String rest = FilenameUtils.normalizeNoEndSeparator(filePath.substring(scheme.end()), useUnix);
    return rest == null ? null : filePath.substring(0, scheme.end()) + rest;
  }

  /**
   * Returns the normalized path without the scheme and authority if the path is a URI (s3://bucket/a/b becomes /a/b), or the
   * given normalized path otherwise.
   */
  private String relativeHierarchyPath(String rawPath, String filePath) {
    Matcher scheme = URI_SCHEME.matcher(rawPath);
    if (!scheme.lookingAt()) {
      return filePath;
    }

    for (int i = scheme.end(); i < rawPath.length(); i++) {
      char ch = rawPath.charAt(i);
      if (ch == '/' || ch == '\\') {
        String relativePath = FilenameUtils.normalizeNoEndSeparator(rawPath.substring(i), useUnix);
        // a path that leaves the root, like s3://bucket/../a, has no hierarchy
        return relativePath == null ? "" : relativePath;
      }
    }

    // only a scheme and authority, like s3://bucket
    return "";
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

  // Counts the segments the same way addHierarchy splits them.
  private int countSegments(String path) {
    int count = 0;
    int pos = skipSeparators(path, 0);

    while (pos < path.length()) {
      count++;
      int end = path.indexOf(sepChar, pos);
      if (end < 0) {
        break;
      }
      pos = skipSeparators(path, end);
    }

    return count;
  }

  private int skipSeparators(String path, int pos) {
    while (pos < path.length() && path.charAt(pos) == sepChar) {
      pos++;
    }
    return pos;
  }
}

