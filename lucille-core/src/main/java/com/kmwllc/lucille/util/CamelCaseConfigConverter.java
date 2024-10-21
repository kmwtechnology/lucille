package com.kmwllc.lucille.util;

import java.io.*;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.text.CaseUtils;

public class CamelCaseConfigConverter {

  private static final Logger log = LoggerFactory.getLogger(CamelCaseConfigConverter.class);

  private static String snakeToCamel(String snakeStr) {
    return CaseUtils.toCamelCase(snakeStr, false, '_');
  }

  private static void writeToConfFile(String configString, String filePath) throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
      writer.printf(configString);
      log.info("Written new config to {}", filePath);
    } catch (Exception e) {
      throw new IOException("Error writing new config to file path: " + filePath, e);
    }
  }


  private static String applyCamelCase(File file) throws IOException {
    StringBuilder sb = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.contains(":")) {
          line = processLine(line, ":");
        } else if (line.contains("=")) {
          line = processLine(line, "=");
        }
        sb.append(line).append("\n");
      }
    } catch (Exception e) {
      throw new IOException("Error processing file: " + file.getName(), e);
    }

    return sb.toString();
  }

  private static String processLine(String line, String toDetect) {
    StringBuilder sb = new StringBuilder();

    // get indent spacing
    int indentSpacing = 0;
    while (indentSpacing < line.length() && line.charAt(indentSpacing) == ' ') {
      indentSpacing++;
    }

    // check if line has snake case to convert
    int splitIndex = line.indexOf(toDetect);
    String key = line.substring(indentSpacing, splitIndex);
    if (!key.contains("_")){
      return line;
    }

    // rebuild line
    key = snakeToCamel(key.trim());
    String value = line.substring(splitIndex);
    sb.append(" ".repeat(indentSpacing))
        .append(key)
        .append(value);

    return sb.toString();
  }

  private static void processFile(String filePath) throws Exception {
    File file = new File(filePath);
    String directory = file.getParent();
    String fileName = file.getName();
    String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));
    String newFileName = nameWithoutExt + "CamelCase" + ".conf";
    String newFilePath = Paths.get(directory, newFileName).toString();

    String configStr = applyCamelCase(file);
    writeToConfFile(configStr, newFilePath);
  }

  private static void validateArgument(String[] args) {
    if (args.length != 1) {
      log.error("Usage: java ConfigProcessor <filepath/To/Conf>");
      System.exit(1);
    }

    String filePath = args[0];
    if (!filePath.endsWith(".conf")) {
      log.error("Invalid file extension, must be used on a config file.");
      System.exit(1);
    }
  }

  public static void main(String[] args) {
    validateArgument(args);

    try {
      processFile(args[0]);
    } catch (Exception e) {
      log.error("Error processing file: {}", e.getMessage());
      System.exit(1);
    }
  }
}
