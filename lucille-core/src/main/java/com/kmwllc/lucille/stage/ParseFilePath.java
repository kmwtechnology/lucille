package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
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
 *   <br> - file_paths: The parent directories of the file (see includeHierarchy).
 *   <br> - level: The number of segments in the path (see includeLevel).
 *   <br> - scheme: The URI scheme, like "s3" (only for a URI, see parseUris).
 *   <br> - authority: The URI authority, like a bucket or host, or "" for file:///a (only for a URI, see parseUris).
 * <p>
 * A path that starts with a URI scheme of two or more letters followed by "://" (like s3://bucket/a/file.txt or
 * file:///a/file.txt) is parsed as a URI: the scheme and authority go in their own fields, and every other field is computed
 * from the URI's path only, so s3://bucket/a/file.txt gives path "/a/file.txt", folder "/a" and file_paths "a". A query or
 * fragment is dropped. A one-letter scheme is not matched, so a Windows drive (C://dir) is parsed as a path. If the URI's path
 * leaves the root (s3://bucket/../a), the whole value is parsed as a path instead.
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
 *   leading up to the full file path (to aid in building a hierarchical aggregator/facet for a search engine). Defaults to true.</li>
 *   <li>parseUris (Boolean, Optional) : If true, a path that starts with a URI scheme is parsed as a URI, as described above. If
 *   false, it is parsed like any other path, which collapses the "://" to ":/" and makes the scheme the first value in
 *   "file_paths" (s3:, s3:/bucket, ...). Defaults to true.</li>
 *   <li>decodeFilePath (Boolean, Optional) : If true, the file path is URL-decoded (UTF-8) before it is parsed. For a URI, the
 *   authority and path are decoded after the URI is split. A "+" is kept as is, and a path with an invalid escape sequence or an
 *   encoded null character (%00) is parsed without decoding. Defaults to false.</li>
 *   <li>includeLevel (Boolean, Optional) : If true, a field ("level") will be populated with the number of segments in the path,
 *   including the file name: 3 for /a/b/file.txt and for C:\a\file.txt. Segments are counted the same way as for "file_paths", so
 *   for a URI only the segments of its path are counted. Defaults to false.</li>
 *   <li>clearInvalidExtension (Boolean, Optional) : If true, "file_extension" is set to "" when the extension contains a control
 *   character or a character that looks like a slash (like U+2215 DIVISION SLASH or U+FF3C FULLWIDTH REVERSE SOLIDUS), which is
 *   not a real extension. Defaults to false.</li>
 *   <li>rootedHierarchy (Boolean, Optional) : If true, each value in "file_paths" starts with fileSep, so /a/b/file.txt gives "/a"
 *   and "/a/b" instead of "a" and "a/b". A Windows path gets one too: C:\a\file.txt gives "\C:" and "\C:\a". Defaults to
 *   false.</li>
 * </ul>
 */
public class ParseFilePath extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("filePathField", "fileSep")
      .optionalBoolean("uppercaseExtension", "includeHierarchy", "parseUris", "decodeFilePath", "includeLevel",
          "clearInvalidExtension", "rootedHierarchy")
      .build();

  // A URI scheme followed by "://". Schemes of one letter are not matched, so that a Windows drive (C://dir) is not taken for one.
  private static final Pattern URI_SCHEME = Pattern.compile("[a-zA-Z][a-zA-Z0-9+.-]+://");

  // "/", "\" and the Unicode characters that look like them
  private static final String SLASH_LIKE_CHARACTERS = "/\\\u2215\u2044\u29F8\uFF0F\uFE68\u2216\u29F9\uFF3C";

  private final String filePathField;
  private final String fileSep;
  private final boolean uppercaseExtension;
  private final boolean includeHierarchy;
  private final boolean parseUris;
  private final boolean decodeFilePath;
  private final boolean includeLevel;
  private final boolean clearInvalidExtension;
  private final boolean rootedHierarchy;
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
    this.parseUris = ConfigUtils.getOrDefault(config, "parseUris", true);
    this.decodeFilePath = ConfigUtils.getOrDefault(config, "decodeFilePath", false);
    this.includeLevel = ConfigUtils.getOrDefault(config, "includeLevel", false);
    this.clearInvalidExtension = ConfigUtils.getOrDefault(config, "clearInvalidExtension", false);
    this.rootedHierarchy = ConfigUtils.getOrDefault(config, "rootedHierarchy", false);
    this.useUnix = fileSep.equals("/");
    this.sepChar = fileSep.charAt(0);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String rawPath = doc.getString(filePathField);
    String filePath = null;

    if (parseUris) {
      UriParts uri = splitUri(rawPath);
      if (uri != null) {
        filePath = FilenameUtils.normalizeNoEndSeparator(decodeIfEnabled(uri.path), useUnix);
        if (filePath != null) {
          doc.addToField("scheme", uri.scheme);
          doc.addToField("authority", decodeIfEnabled(uri.authority));
        }
      }
    }

    if (filePath == null) {
      filePath = FilenameUtils.normalizeNoEndSeparator(decodeIfEnabled(rawPath), useUnix);
    }

    doc.addToField("filename", FilenameUtils.getName(filePath));
    doc.addToField("folder", FilenameUtils.getFullPathNoEndSeparator(filePath));
    doc.addToField("path", filePath);

    String extension = FilenameUtils.getExtension(filePath);
    if (clearInvalidExtension && isInvalidExtension(extension)) {
      extension = "";
    }

    if (uppercaseExtension) {
      doc.addToField("file_extension", extension.toUpperCase());
    } else {
      doc.addToField("file_extension", extension);
    }

    if (includeLevel) {
      doc.addToField("level", countSegments(filePath));
    }

    if (includeHierarchy) {
      addHierarchy(doc, filePath);
    }

    return null;
  }

  private String decodeIfEnabled(String value) {
    return decodeFilePath ? decode(value) : value;
  }

  private static String decode(String filePath) {
    String decoded;
    try {
      // URLDecoder decodes "+" to a space, which is only correct for form data. Encode it so it is kept in the path.
      decoded = URLDecoder.decode(filePath.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      log.warn("Could not URL-decode file path {}, parsing it without decoding: {}", filePath, e.getMessage());
      return filePath;
    }

    // a path can't contain a null character, and FilenameUtils rejects one
    if (decoded.indexOf('\0') >= 0) {
      log.warn("URL-decoding file path {} gives a null character, parsing it without decoding", filePath);
      return filePath;
    }

    return decoded;
  }

  private static boolean isInvalidExtension(String extension) {
    if (extension == null) {
      return false;
    }

    for (int i = 0; i < extension.length(); i++) {
      char ch = extension.charAt(i);
      if (Character.isISOControl(ch) || SLASH_LIKE_CHARACTERS.indexOf(ch) >= 0) {
        return true;
      }
    }

    return false;
  }

  /**
   * Splits a path that starts with a URI scheme into its scheme, raw authority and raw path, or returns null if it doesn't start
   * with one. java.net.URI is used when it accepts the value, so a query or fragment is dropped. Otherwise (for example, a raw
   * space or a backslash), the authority runs up to the first "/" or "\" after the "://", and the rest is the path.
   */
  private static UriParts splitUri(String rawPath) {
    Matcher scheme = URI_SCHEME.matcher(rawPath);
    if (!scheme.lookingAt()) {
      return null;
    }

    String schemeName = rawPath.substring(0, scheme.end() - "://".length());

    try {
      URI uri = new URI(rawPath);
      return new UriParts(schemeName, Objects.toString(uri.getRawAuthority(), ""), Objects.toString(uri.getRawPath(), ""));
    } catch (URISyntaxException e) {
      int pathStart = scheme.end();
      while (pathStart < rawPath.length() && rawPath.charAt(pathStart) != '/' && rawPath.charAt(pathStart) != '\\') {
        pathStart++;
      }
      return new UriParts(schemeName, rawPath.substring(scheme.end(), pathStart), rawPath.substring(pathStart));
    }
  }

  private record UriParts(String scheme, String authority, String path) {
  }

  /**
   * Adds each proper prefix of the path's segments to "file_paths". Segments are split the same way as
   * StringUtils.split(path, fileSep): runs of separators never yield an empty segment, and leading or trailing
   * separators are dropped. The prefixes are built in one growing StringBuilder, with one append per segment,
   * instead of re-joining all of the earlier segments for every prefix. If rootedHierarchy is set, each prefix starts with
   * fileSep.
   */
  private void addHierarchy(Document doc, String path) {
    StringBuilder prefix = new StringBuilder(path.length() + 1);
    if (rootedHierarchy) {
      prefix.append(sepChar);
    }
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

