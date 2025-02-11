package com.kmwllc.lucille.util;

import java.util.Collection;
import org.apache.commons.lang3.ThreadUtils;

public class ThreadNameUtils {
  public static final String THREAD_NAME_PREFIX = "Lucille";

  /**
   * Create a Thread name with the given name.
   */
  public static String createName(String name) {
    return createName(name, null);
  }

  /**
   * Create a Thread name with the given name and runId associated. If the given runId is null, the thread name
   * will just be the base prefix and the given name.
   */
  public static String createName(String name, String runId) {
    if (runId == null) {
      return THREAD_NAME_PREFIX + "-" + name;
    } else {
      return THREAD_NAME_PREFIX + "-" + runId + "-" + name;
    }
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
