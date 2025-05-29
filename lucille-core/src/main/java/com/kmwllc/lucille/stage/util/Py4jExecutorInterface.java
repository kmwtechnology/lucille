package com.kmwllc.lucille.stage.util;

/**
 * Interface for executing Python code from Java via Py4J.
 * <p>
 * Implementations of this interface are expected to be exposed as entry points to the Py4J GatewayServer
 * and provide a method for Java to send JSON-encoded messages or commands to Python for execution.
 * The typical use case is to pass a JSON string representing a method call and its arguments, which the Python
 * side will decode, dispatch, and execute, returning the result (often as a JSON string) back to Java.
 * </p>
 */
public interface Py4jExecutorInterface  {

  /**
   * Executes a Python method call based on a JSON-encoded message sent from Java.
   * <p>
   * The {@code jsonEncodedMessage} must be a JSON string representing an object with at least the following fields:
   * <ul>
   *   <li><b>method</b> (String): The name of the Python function to call (e.g., "process_document").</li>
   *   <li><b>data</b> (Object): The argument to pass to the Python function, typically a document represented as a map or JSON object.</li>
   * </ul>
   * <p>
   * Example:
   * <pre>
   *   {
   *     "method": "process_document",
   *     "data": { ...document fields... }
   *   }
   * </pre>
   * The Python implementation should decode this message, dispatch the call to the specified function,
   * and return the result (often as a JSON string) back to Java.
   *
   * @param jsonEncodedMessage JSON string specifying the method to call and its arguments.
   * @return The result of the Python function, typically as a JSON string.
   */
  public Object exec(String jsonEncodedMessage);

}
