package com.kmwllc.lucille.connector;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.github.vfss3.S3FileSystemConfigBuilder;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @deprecated
 * This Connector is deprecated and will be removed in future releases. Use FileConnector instead.
 */
@Deprecated
public class VFSConnector extends AbstractConnector {

  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static final String CONTENT = "file_content";

  private static final Logger log = LoggerFactory.getLogger(VFSConnector.class);

  private final String vfsPath;
  private final List<Pattern> includes;
  private final List<Pattern> excludes;

  public VFSConnector(Config config) throws ConnectorException {
    super(config);

    // normalize vfsPath to convert to a URI even if they specified an absolute or relative local file path
    String rawPath = config.getString("vfsPath");
    vfsPath = FileUtils.isValidURI(rawPath) ? rawPath : Path.of(rawPath).normalize().toUri().toString();

    // Compile include and exclude regex paths or set an empty list if none were provided (allow all files)
    List<String> includeRegex = config.hasPath("includes") ?
        config.getStringList("includes") : Collections.emptyList();
    includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = config.hasPath("excludes") ?
        config.getStringList("excludes") : Collections.emptyList();
    excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());

  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try (StandardFileSystemManager fsManager = new StandardFileSystemManager()) {
      // ensure kube access role support to work around issue https://github.com/abashev/vfs-s3/issues/77
      FileSystemOptions fileSystemOptions = new FileSystemOptions();
      S3FileSystemConfigBuilder s3Config = S3FileSystemConfigBuilder.getInstance();
      s3Config.setCredentialsProvider(fileSystemOptions, new DefaultAWSCredentialsProviderChain());

      fsManager.init();

      // locate all valid files within the provided VFS path in order
      traverseFiles(fsManager, vfsPath).forEachOrdered(fo -> {
        final Document doc = buildDocument(fo);
        try {
          publisher.publish(doc);
        } catch (Exception e) {
          log.error("Unable to publish document '" + fo.getPublicURIString() + "', SKIPPING", e);
        }
      });

    } catch (FileSystemException e) {
      throw new ConnectorException("Unable to get VFS Manager object", e);
    }
  }

  private Document buildDocument(FileObject fo) {
    final String docId = DigestUtils.md5Hex(fo.getName().getPath());
    final Document doc = Document.create(createDocId(docId));

    // Set up basic file properties on the doc
    doc.setField(FILE_PATH, fo.getName().getURI());

    // get access to file content
    try (FileContent content = fo.getContent()) {
      doc.setField(MODIFIED, Instant.ofEpochMilli(content.getLastModifiedTime()).toString());
      doc.setField(CREATED, Instant.ofEpochMilli(content.getLastModifiedTime()).toString());
      doc.setField(SIZE, content.getSize());
      doc.setField(CONTENT, content.getByteArray());
    } catch (FileSystemException e) {
      doc.setField("error", e.toString());
      log.error("Issue getting file information from '" + fo.getName() + "'", e);
    } catch (IOException e) {
      doc.setField("error", e.toString());
      log.error("Issue getting file content from '" + fo.getName() + "'", e);
    }
    return doc;
  }

  private Stream<FileObject> traverseFiles(FileSystemManager fsManager, String rootPath) throws ConnectorException {
    FileObject fileRoot;

    // get the root object, file or directory
    try {
      fileRoot = fsManager.resolveFile(rootPath);
    } catch (FileSystemException e) {
      throw new ConnectorException("Unable to resolve vsfPath of '" + rootPath + "'", e);
    }

    // return filtered FileObject stream
    return StreamSupport.stream(fileRoot.spliterator(), false)
        .filter(this::isValidFile);

  }

  private boolean isValidFile(FileObject fo) {
    // skip if anything other than a file
    try {
      if (fo.getType() != FileType.FILE) {
        return false;
      }
    } catch (FileSystemException e) {
      log.error("Problem getting VFS FileType from '" + fo.getName() + "'", e);
      return false;
    }

    // skip anything that matches excludes or doesn't match includes
    String filePath = fo.getName().getPath();
    if (excludes.stream().anyMatch(pattern -> pattern.matcher(filePath).matches())
        || (!includes.isEmpty() && includes.stream().noneMatch(pattern -> pattern.matcher(filePath).matches()))) {
      log.debug("Skipping file because of include or exclude regex: " + filePath);
      return false;
    }

    // everything else should be good
    return true;
  }
}
