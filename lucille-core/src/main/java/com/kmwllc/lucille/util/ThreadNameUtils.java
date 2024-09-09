package com.kmwllc.lucille.util;

public class ThreadNameUtils {
  public static final String THREAD_NAME_PREFIX = "Lucille";


  public static String setThreadPrefix(String name) {
    return THREAD_NAME_PREFIX + "-" + name;
  }

  public static boolean isLucilleThread(Thread thread) {return thread.getName().startsWith(THREAD_NAME_PREFIX);}
}
