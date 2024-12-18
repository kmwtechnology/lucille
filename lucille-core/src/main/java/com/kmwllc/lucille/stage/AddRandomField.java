package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Adds random data to a document field given parameters. Note that when randomly selecting multiple values
 * from an integer range or from file contents, duplicates are possible -- this stage does not currently guarantee
 * that all random terms added to a field will be distinct.
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>inputDataPath</b> (String, Optional) : file path to a text file that stores datapoints to be randomly placed into field,
 *  defaults to numeric data based on range size (0 -&gt; rangeSize - 1). Note that duplicate entries will not be removed.
 * </p>
 * <p>
 * <b>fieldName</b> (String, Optional) : Field name of field where data is placed, defaults to "data"
 *  </p>
 * <p>
 * <b>rangeSize</b> (int, Optional) : size of the subset of datapoints to be grabbed either from
 *  given datapath or from random numbers
 * </p>
 *  <p>
 * <b>minNumOfTerms</b> (Integer, Optional) : minimum number of terms to be in the field, defaults to 1
 * </p>
 * <p>
 * <b>maxNumOfTerms</b> (Integer, Optional) : maximum number of terms to be in the field, defaults to 1
 * </p>
 * <p>
 * <b>isNested</b> (bool, Optional) : setting for structure of field, default or nested
 * <p>
 * <b>concatenate</b> (bool, Optional) : if true, represent multiple terms as a single space-separated string instead of multiple
 *  values, defaults to false
 * </p>
 */
public class AddRandomField extends Stage {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String inputDataPath;
  private final String fieldName;
  private Integer rangeSize;
  private Integer minNumOfTerms;
  private Integer maxNumOfTerms;
  private final boolean isNested;
  private List<String> fileData;
  private final boolean concatenate;

  public AddRandomField(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("input_data_path", "field_name", "range_size", "min_num_of_terms",
        "max_num_of_terms", "is_nested", "concatenate"));
    this.inputDataPath = ConfigUtils.getOrDefault(config, "input_data_path", null);
    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.minNumOfTerms = ConfigUtils.getOrDefault(config, "min_num_of_terms", null);
    this.maxNumOfTerms = ConfigUtils.getOrDefault(config, "max_num_of_terms", null);
    this.isNested = ConfigUtils.getOrDefault(config, "is_nested", false);
    this.concatenate = ConfigUtils.getOrDefault(config, "concatenate", false);
    this.rangeSize = ConfigUtils.getOrDefault(config, "range_size", null);
    this.fileData = null;

    if (this.minNumOfTerms == null ^ this.maxNumOfTerms == null) {
      throw new StageException("Both minimum and maximum number of terms must be specified if either is specified");
    }
    if (this.minNumOfTerms == null && this.maxNumOfTerms == null) {
      this.minNumOfTerms = 1;
      this.maxNumOfTerms = 1;
    }
    if (this.minNumOfTerms > this.maxNumOfTerms) {
      throw new StageException("Minimum number of terms must be less than or equal to maximum");
    }
    if (inputDataPath == null && rangeSize == null) {
      throw new StageException("range_size must be specified if there is no input_data_path");
    }
    if (concatenate && isNested) {
      throw new StageException("concatenate=true is not currently supported when is_nested=true");
    }
  }

  @Override
  public void start() throws StageException {
    fileData = (inputDataPath != null) ? getFileData(inputDataPath) : null;

    if (fileData != null && rangeSize != null) {
        if (rangeSize > fileData.size()) {
          throw new StageException("Range size must be less than the number of lines in given file");
        }

        if (rangeSize < fileData.size()) {
          // shuffle contents so when we truncate we're not always using the initial elements
          // TODO: might be slow for large files, consider whether this is actually needed
          Collections.shuffle(fileData);
          fileData = fileData.subList(0, rangeSize);
        }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    int fieldLength = getRandomFieldLength(minNumOfTerms, maxNumOfTerms);

    List<String> data = (fileData == null) ? getRandomIntegerStringList(fieldLength, rangeSize) :
        selectRandomElements(fileData, fieldLength);

    if (isNested) {
      ArrayNode array = MAPPER.createArrayNode();
      // TODO: concatenate option not yet supported with nested mode; need tests for nested mode
      for (String value : data) {
        array.add(MAPPER.createObjectNode().put("data", value));
      }
      doc.setField(fieldName, array);
    } else {
      if (concatenate) {
        doc.setOrAdd(fieldName, String.join(" ", data));
      } else {
        for (String value : data) {
          doc.setOrAdd(fieldName, value);
        }
      }
    }
    return null;
  }

  private static List<String> getFileData(String inputDataPath) throws StageException {
    try {
      return Files.readAllLines(Path.of(inputDataPath));
    } catch (IOException e) {
      throw new StageException("Could not read provided file path", e);
    }
  }

  private static List<String> getRandomIntegerStringList(int length, int rangeSize) {
    List<String> result = new ArrayList();
    for (int i = 0; i < length; i++) {
      result.add(String.valueOf(ThreadLocalRandom.current().nextInt(0, rangeSize)));
    }
    return result;
  }

  private static List<String> selectRandomElements(List<String> values, int length) {
    ArrayList<String> fieldData = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      fieldData.add(values.get(ThreadLocalRandom.current().nextInt(values.size())));
    }
    return fieldData;
  }

  private static int getRandomFieldLength(int minInclusive, int maxInclusive) {
    return ThreadLocalRandom.current().nextInt(minInclusive, maxInclusive+1);
  }
}
