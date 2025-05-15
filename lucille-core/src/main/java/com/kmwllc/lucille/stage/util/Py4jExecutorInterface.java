package com.kmwllc.lucille.stage.util;

public interface Py4jExecutorInterface  {

  /**
   * Lets the Python Py4jClient setup with configuration
   * @param jsonEncodedConfig - json encoded configuration
   */
  public void start(String jsonEncodedConfig);

  /**
   * exec in Python - executes arbitrary code
   * @param code
   * @return
   */
  public Object exec(String code);


  /**
   * Send interface which takes a json encoded Message.
   * For schema look at org.myrobotlab.framework.Message
   * @param jsonEncodedMessage - json encoded Document Message
   */
  public Object send(String jsonEncodedMessage);

}
