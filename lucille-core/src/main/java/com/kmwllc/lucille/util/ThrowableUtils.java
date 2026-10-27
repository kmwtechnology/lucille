package com.kmwllc.lucille.util;

/**
 * Classification of Throwables shared by the components that must decide whether to carry on after one.
 */
public final class ThrowableUtils {

  private ThrowableUtils() {
  }

  /**
   * Whether the Throwable means the JVM itself can no longer be relied on: an OutOfMemoryError, InternalError or
   * UnknownError. A component that catches one should record it, clean up and stop, not continue with the next unit of
   * work. A StackOverflowError is also a VirtualMachineError, but the stack has unwound by the time it is caught, so it
   * is as local to the work that caused it as any Exception; every other Error (LinkageError, AssertionError) is local
   * as well.
   */
  public static boolean isFatal(Throwable t) {
    return t instanceof VirtualMachineError && !(t instanceof StackOverflowError);
  }
}
