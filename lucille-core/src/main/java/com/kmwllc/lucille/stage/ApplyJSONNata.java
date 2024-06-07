package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
public class ApplyJSONNata extends Stage {

  private final String source;
  private final String destination;
  private final String expression;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expression = config.getString("expression");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(this.source)) {
      return null;
    }
    
    Expressions.parse("string");
    



    return null;
  }

}
