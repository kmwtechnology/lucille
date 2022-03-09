package com.kmwllc.lucille.stage.EE;

import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Creates a dictionary of alphanumeric strings for testing purposes.
 */
public class CreateDictionary {
  public static void main(String[] args) throws IOException {
    File f = new File("large_dictionary.csv");
    FileWriter writer = new FileWriter("large_dictionary.csv");

    for (int i = 0; i < 1000000; i++) {
      String s1 = RandomStringUtils.randomAlphanumeric(10);
      String s2 = RandomStringUtils.randomAlphanumeric(10);
      writer.write(s1 + "," + s2 + "\n");
    }
    writer.write("zzzzzzzzzz,aaaaaaaaaa");
    writer.close();
  }
}
