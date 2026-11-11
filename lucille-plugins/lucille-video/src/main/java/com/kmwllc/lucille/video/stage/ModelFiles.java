package com.kmwllc.lucille.video.stage;

import com.kmwllc.lucille.core.StageException;
import java.io.File;

final class ModelFiles {

  private ModelFiles() {
  }

  static String require(String path, String label) throws StageException {
    if (path == null || path.isBlank() || !new File(path).isFile()) {
      throw new StageException(label + " does not exist: " + path);
    }
    return path;
  }
}
