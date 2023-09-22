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
import java.util.Set;


/**
 * Fetches byte data of a given URL field and places data into a specified field
 * <br>
 * Config Parameters -
 * <br>
 * inputDataPath (String, Optional) : file path to a text file that stores datapoints to be randomly placed into field,
 *  defaults to numeric data based on cardinality
 * fieldName (String, Optional) : Field name of field where data is placed, defaults to "data"
 * cardinality (int, Optional) : size of the subset of datapoints to be grabbed either from
 *  given datapath or from random numbers
 * minNumOfTerms (Integer, Optional) : minimum number of terms to be in the field, defaults to null
 * maxNumOfTerms (Integer, Optional) : maximum number of terms to be in the field, defaults to null
 * fieldType (FieldType, Optional) : setting for type of field, default or nested, allows for further settings to be easily added
 */
public class AddRandomField extends Stage {

  enum FieldType {
    DEFAULT, NESTED
  }

  private final String inputDataPath;
  private final String fieldName;
  private final int cardinality;
  private Integer minNumOfTerms;
  private Integer maxNumOfTerms;
  private final FieldType fieldtype;
  private final List<String> dataArr;
  private final List<String> uniqueValues;

  public AddRandomField(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("input_data_path", "field_name", "cardinality", "min_num_of_terms",
        "max_num_of_terms", "field_type"));
    this.inputDataPath = ConfigUtils.getOrDefault(config, "input_data_path", null);
    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.minNumOfTerms = ConfigUtils.getOrDefault(config, "min_num_of_terms", null);
    this.maxNumOfTerms = ConfigUtils.getOrDefault(config, "max_num_of_terms", null);
    this.fieldtype = FieldType.valueOf(ConfigUtils.getOrDefault(config, "field_type", "default"));
    this.dataArr = this.inputDataPath != null ? getFileData(this.inputDataPath) : null;
    this.cardinality = ConfigUtils.getOrDefault(config, "cardinality",
        this.dataArr != null ? this.dataArr.size() : this.minNumOfTerms);
    this.uniqueValues = getUniqueValues(this.dataArr != null, this.dataArr);

    if (this.minNumOfTerms == null ^ this.maxNumOfTerms == null) {
      throw new StageException("Both minimum and maximum number of terms must be specified");
    }
    if (this.minNumOfTerms == null && this.maxNumOfTerms == null) {
      this.minNumOfTerms = 1;
      this.maxNumOfTerms = 1;
    }
    if (this.minNumOfTerms > this.maxNumOfTerms) {
      throw new StageException("Minimum number of terms must be less than or equal to maximum");
    }
    if (this.dataArr.size() < this.cardinality) {
      throw new StageException("Cardinality must be less than the number of lines in given file");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    ArrayList<String> fieldDataArr = genFieldDataArr(uniqueValues, minNumOfTerms, maxNumOfTerms);
    Document populatedDoc = null;
    switch (fieldtype) {
      case NESTED:
        populateFieldsNested(fieldName, doc, fieldDataArr);
      case DEFAULT:
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
    for (String value : fieldDataArr) {
      doc.setOrAdd(fieldName, value);
    }
  }

  private List<String> getFileData(String inputDataPath) throws StageException {
    List<String> data = null;
    try {
      data = Files.readAllLines(Path.of(inputDataPath));
    } catch (IOException e) {
      throw new StageException("Could not read provided file path");
    }
    return data;
  }

  private ArrayList<String> getUniqueValues(boolean dataExists, List<String> inputData) {
    ArrayList<String> uniqueValues = null;
    if (!dataExists) {

      // create set of unique values based on given cardinality and input data
      Set<String> uniqueValuesSet = new HashSet<>();
      for (int i = 0; i < cardinality; i++) {
        int randomPos = (int) (Math.random() * inputData.size());
        uniqueValuesSet.add(inputData.get(randomPos));
        inputData.remove(randomPos);
      }
      uniqueValues = new ArrayList<>(uniqueValuesSet);
    } else {
      uniqueValues = new ArrayList<>();
      for (int i = 0; i < cardinality; i++) {
        int randomPos = (int) (Math.random() * cardinality);
        uniqueValues.add(Integer.toString(randomPos));
      }
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
