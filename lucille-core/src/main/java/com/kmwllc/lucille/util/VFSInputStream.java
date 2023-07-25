package com.kmwllc.lucille.util;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;

/** Opens an input stream to a VFS file, then closes the VFS resources when the stream is closed. */
class VFSInputStream extends FilterInputStream {

  private final FileSystemManager manager;
  private final FileObject file;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  VFSInputStream(InputStream in, FileObject file, FileSystemManager manager) {
    super(in);
    this.manager = manager;
    this.file = file;
  }

  static VFSInputStream open(String uri) throws FileSystemException {
    StandardFileSystemManager manager = new StandardFileSystemManager();
    FileObject file = null;
    InputStream in;
    try {
      manager.init();
      file = manager.resolveFile(uri);
      in = file.getContent().getInputStream();
    } catch (Exception e) {
      closeQuietly(file);
      closeQuietly(manager);
      throw e;
    }
    return new VFSInputStream(in, file, manager);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      closeQuietly(file);
      closeQuietly(manager);
    }
  }

  private static void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      log.warn("Error closing resource", e);
    }
  }
}
