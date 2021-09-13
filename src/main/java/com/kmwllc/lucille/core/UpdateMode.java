package com.kmwllc.lucille.core;

public enum UpdateMode {
  APPEND("append"), OVERWRITE("overwrite"), SKIP("skip");

  private String text;

  UpdateMode(String text) {
    this.text = text;
  }

  public static UpdateMode fromString(String modeStr) {
    for (UpdateMode mode : UpdateMode.values()) {
      if (modeStr.equals(mode.text)) {
        return mode;
      }
    }

    return null;
  }

}
