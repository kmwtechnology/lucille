package com.kmwllc.lucille.util;

public class ThreadNameUtils {
  public static final String THREAD_NAME_PREFIX = "Lucille";


  public static String getThreadName(String name) {
    return THREAD_NAME_PREFIX + "-" + name;
  }
}
