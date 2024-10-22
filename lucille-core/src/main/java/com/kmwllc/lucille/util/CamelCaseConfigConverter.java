package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  private static String applyCamelCase(File file, List<String> stageProperties) throws IOException {
    StringBuilder sb = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        for (String stageProperty : stageProperties) {
          if (line.contains(stageProperty)) {
            log.debug("{}: {}", stageProperty, line);
            line = processLine(line, stageProperty);
          }
        }
        sb.append(line).append("\n");
      }
    } catch (Exception e) {
      throw new IOException("Error processing file: " + file.getName(), e);
    }

    return sb.toString();
  }

  private static String processLine(String line, String stageProperty) throws Exception {
    StringBuilder sb = new StringBuilder();
    String[] result = line.split(stageProperty);
    if (result.length > 2) {
      throw new Exception("Line contains multiple instances of stageProperty");
    }
    sb.append(result[0]).append(snakeToCamel(stageProperty)).append(result[1]);

    return sb.toString();
  }

  private static void processFile(String filePath) throws Exception {
    File file = new File(filePath);
    String directory = file.getParent();
    String fileName = file.getName();
    String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));
    String newFileName = nameWithoutExt + "CamelCase" + ".conf";
    String newFilePath = Paths.get(directory, newFileName).toString();

    // collect all stageProperty that needs to change
    Config config = ConfigFactory.parseFile(file);
    Map<String, Object> configAsMap = config.root().unwrapped();
    List<String> stageProperties = getStagePropertiesFromConfig(configAsMap, fileName);

    // update stage properties in new file and write to newFilePath
    String configStr = applyCamelCase(file, stageProperties);
    writeToConfFile(configStr, newFilePath);
  }

  private static List<String> getStagePropertiesFromConfig(Map<String, Object> configAsMap, String fileName) throws IOException {
    List<String> stageProperties = new ArrayList<>();
    try {
      List<Map<String, Object>> pipelinesMap = (List<Map<String, Object>>) configAsMap.get("pipelines");
      for (Map<String, Object> pipeline : pipelinesMap) {
        List<Map<String, Object>> stages = (List<Map<String, Object>>) pipeline.get("stages");
        for (Map<String, Object> stage : stages) {
          List<String> properties = stage.keySet().stream()
              // to collect only those properties that only need to be updated
              .filter(key -> !key.equals("name") && !key.equals("class") && key.contains("_"))
              .toList();
          stageProperties.addAll(properties);
        }
      }
    } catch (Exception e) {
      throw new IOException("Error getting stage properties from config: " + fileName, e);
    }

    return stageProperties;
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
