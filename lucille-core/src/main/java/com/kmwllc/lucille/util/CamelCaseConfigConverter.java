package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.text.CaseUtils;

/**
 * This utility class converts snake_case stage properties to camelCase in a given config file. Note that this script will
 * not work if you have UNSET environment variables in your config file. Make sure you have set your environment variables set before
 * running this.
 *
 * NOTE: This does not target nested configs for stages such as AddRandomNestedField.
 *
 * <p>
 * Usage Example: java -cp 'lucille.jar:lib/*' com.kmwllc.lucille.util.CamelCaseConfigConverter /path/to/config.conf
 */
public class CamelCaseConfigConverter {

  private static final Logger log = LoggerFactory.getLogger(CamelCaseConfigConverter.class);

  private static String snakeToCamel(String snakeStr) {
    return CaseUtils.toCamelCase(snakeStr, false, '_');
  }

  private static void writeToConfFile(String configString, String filePath) throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
      writer.print(configString);
      log.info("Written new config to {}", filePath);
    } catch (Exception e) {
      throw new IOException("Error writing new config to file path: " + filePath, e);
    }
  }

  private static String applyCamelCase(File file, List<String> stageProperties) throws IOException {
    StringBuilder sb = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      // iterating over lines for the edge case where the stage property is used one or more times as a property value in the same line
      // in this case we want to replace only the first occurrence -- which is the stage property
      // StringUtils.replaceEach does not give a way to limiting the number of replacements of the same value in the same line/config
      while ((line = br.readLine()) != null) {
        for (String stageProperty : stageProperties) {
          if (line.contains(stageProperty)) {
            line = StringUtils.replaceOnce(line, stageProperty, snakeToCamel(stageProperty));
          }
        }
        sb.append(line).append("\n");
      }
    } catch (Exception e) {
      throw new IOException("Error processing file: " + file.getName(), e);
    }

    return sb.toString();
  }

  private static void processFile(String filePath) throws Exception {
    String newFileName = FilenameUtils.getBaseName(filePath) + "CamelCase" + ".conf";
    String newFilePath = Paths.get(FilenameUtils.getFullPath(filePath), newFileName).toString();
    // validate canonical path of filePath for Path Traversal vulnerability
    String baseDir = newFilePath.replace(newFileName, "");
    String canonicalBasePath = new File(baseDir).getCanonicalPath();
    String canonicalFilePath = new File(newFilePath).getCanonicalPath();
    if (!canonicalFilePath.startsWith(canonicalBasePath)) {
      throw new SecurityException("Invalid file path: " + filePath);
    }

    File file = new File(filePath);
    // collect all stageProperty that needs to change
    Config config = ConfigFactory.parseFile(file).resolve();
    Map<String, Object> configAsMap = config.root().unwrapped();
    List<String> stageProperties = getStagePropertiesFromConfig(configAsMap);

    // update stage properties in new file and write to newFilePath
    String configStr = applyCamelCase(file, stageProperties);
    writeToConfFile(configStr, canonicalFilePath);
  }

  private static List<String> getStagePropertiesFromConfig(Map<String, Object> configAsMap) throws IOException {
    List<String> stageProperties = new ArrayList<>();
    try {
      List<Map<String, Object>> pipelinesMap = (List<Map<String, Object>>) configAsMap.get("pipelines");
      for (Map<String, Object> pipeline : pipelinesMap) {
        List<Map<String, Object>> stages = (List<Map<String, Object>>) pipeline.get("stages");
        for (Map<String, Object> stage : stages) {
          List<String> properties = stage.keySet().stream()
              // to collect only those properties that only need to be updated
              .filter(key -> key.contains("_"))
              .toList();
          stageProperties.addAll(properties);
        }
      }
    } catch (Exception e) {
      throw new IOException("Error getting stage properties from config.", e);
    }

    return stageProperties;
  }

  private static void validateArguments(String[] args) {
    if (args.length != 1) {
      log.error("Invalid number of arguments, must provide a path to a config file.\n"
          + "Usage Example: java -cp 'lucille.jar:lib/*' com.kmwllc.lucille.util.CamelCaseConfigConverter /path/to/config.conf");
      System.exit(1);
    }

    String filePath = args[0];
    File file = new File(filePath);
    if (!file.exists()) {
      log.error("file does not exists: {}", filePath);
      System.exit(1);
    }
    if (!filePath.endsWith(".conf")) {
      log.error("Invalid file extension, must be used on a config file.");
      System.exit(1);
    }
  }

  public static void main(String[] args) {
    validateArguments(args);

    try {
      processFile(args[0]);
    } catch (Exception e) {
      log.error("Error processing file: ", e);
      System.exit(1);
    }
  }
}