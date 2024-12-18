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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Adds random data to a document field given parameters
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>inputDataPath</b> (String, Optional) : file path to a text file that stores datapoints to be randomly placed into field,
 *  defaults to numeric data based on range size (0 -&gt; rangeSize - 1)
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
 * <b>isNested</b> (FieldType, Optional) : setting for structure of field, default or nested
 * </p>
 */
public class AddRandomField extends Stage {

  private final String inputDataPath;
  private final String fieldName;
  private Integer rangeSize;
  private Integer minNumOfTerms;
  private Integer maxNumOfTerms;
  private final boolean isNested;
  private List<String> dataArr;
  private List<String> uniqueValues;
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
    this.dataArr = null;
    this.rangeSize = null;
    this.uniqueValues = null;

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
  }

  @Override
  public void start() throws StageException {
    this.dataArr = this.inputDataPath != null ? getFileData(this.inputDataPath) : null;
    this.rangeSize = ConfigUtils.getOrDefault(config, "range_size",
        this.dataArr != null ? this.dataArr.size() : this.maxNumOfTerms);
    this.uniqueValues = getUniqueValues(this.dataArr != null, this.dataArr);

    if (this.dataArr != null && this.rangeSize > this.dataArr.size()) {
      throw new StageException("Range size must be less than the number of lines in given file");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    ArrayList<String> fieldDataArr = genFieldDataArr(uniqueValues, minNumOfTerms, maxNumOfTerms);
    if (isNested) {
      populateFieldsNested(fieldName, doc, fieldDataArr);
    } else {
      populateFieldsDefault(fieldName, doc, fieldDataArr);
    }
    return null;
  }

  private void populateFieldsNested(String fieldName, Document doc, ArrayList<String> fieldDataArr) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode array = mapper.createArrayNode();
    for (String value : fieldDataArr) {
      array.add(mapper.createObjectNode().put("data", value));
    }
    doc.setField(fieldName, array);
  }

  private void populateFieldsDefault(String fieldName, Document doc, ArrayList<String> fieldDataArr) {
    if (concatenate) {
      doc.setOrAdd(fieldName, String.join(" ", fieldDataArr));
    } else {
      for (String value : fieldDataArr) {
        doc.setOrAdd(fieldName, value);
      }
    }
  }

  private List<String> getFileData(String inputDataPath) throws StageException {
    try {
      List<String> data = Files.readAllLines(Path.of(inputDataPath));
      return data;
    } catch (IOException e) {
      throw new StageException("Could not read provided file path", e);
    }
  }

  private List<String> getUniqueValues(boolean dataExists, List<String> inputData) {
    List<String> uniqueValues = new ArrayList<>();
    if (dataExists) {
      List<String> initialData = new ArrayList(new HashSet<>(inputData));
      // create set of unique values based on given range size and input data
      for (int i = 0; i < rangeSize; i++) {
        int randomPos = (int) (Math.random() * initialData.size());
        uniqueValues.add(initialData.get(randomPos));
        initialData.remove(randomPos);
      }
    } else {
      // create sequential list of numbers ending at range size
      List<Integer> seqList = IntStream.range(0, rangeSize)
          .boxed().collect(Collectors.toList());
      uniqueValues = seqList.stream().map(i -> i.toString()).collect(Collectors.toList());
    }
    return uniqueValues;
  }

  private ArrayList<String> genFieldDataArr(List<String> uniqueValues, int minNumOfTerms, int maxNumOfTerms) {
    ArrayList<String> fieldData = new ArrayList<>();
    int fieldDataSize = (int) ((Math.random() * (maxNumOfTerms - minNumOfTerms)) + minNumOfTerms);
    for (int i = 0; i < fieldDataSize; i++) {
      fieldData.add(uniqueValues.get((int) (Math.random() * uniqueValues.size())));
    }
    return fieldData;
  }
}
