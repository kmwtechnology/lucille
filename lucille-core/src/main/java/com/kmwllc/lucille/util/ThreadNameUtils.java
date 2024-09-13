package com.kmwllc.lucille.util;

import java.util.Collection;
import org.apache.commons.lang3.ThreadUtils;

public class ThreadNameUtils {
  public static final String THREAD_NAME_PREFIX = "Lucille";


  public static String createName(String name) {
    return THREAD_NAME_PREFIX + "-" + name;
  }

  public static boolean isLucilleThread(Thread thread) {return thread.getName().startsWith(THREAD_NAME_PREFIX);}

  public static boolean areLucilleThreadsRunning() {
    Collection<Thread> nonSystemThreads =
        ThreadUtils.findThreads(t -> !ThreadUtils.getSystemThreadGroup().equals(t.getThreadGroup()));

    for (Thread thread : nonSystemThreads) {
      if (isLucilleThread(thread)) {
        return true;
      }
    }
    return false;
  }
}
