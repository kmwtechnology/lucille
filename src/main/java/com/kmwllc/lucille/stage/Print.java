package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This Stage will Print all documents in the pipeline to the Logger.
 * <p>
 * Config Parameters -
 * <p>
 * - maxFieldSize (Integer, Optional) : The maximum number of characters to print for any field. Defaults to 256.
 * - outputFile (String, Optional) : An optional file to additionally log the documents to.
 * - format (String, Optional) : Determines the format the documents are printed in. Can be 'json' or 'csv'. Defaults
 * to 'json'.
 * - fields (List<String>, Optional) : Determines which fields will be extracted and printed from the Document.
 * By Default, all fields are outputted.
 */
// TODO : Add support for trimming field sizes to a certain length
public class Print extends Stage {

  private final int maxFieldSize;
  private final String outputFile;
  private final String format;
  private final List<String> fields;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private FileWriter writer;

  public Print(Config config) {
    super(config);
    this.maxFieldSize = config.hasPath("maxFieldSize") ? config.getInt("maxFieldSize") : 256;
    this.outputFile = config.hasPath("outputFile") ? config.getString("outputFile") : "";
    this.format = config.hasPath("format") ? config.getString("format").toLowerCase() : "json";
    this.fields = config.hasPath("fields") ? config.getStringList("fields") : new ArrayList<>();
  }

  public void start() throws StageException {
    if (!format.equals("json") && !format.equals("csv")) {
      throw new StageException("Format must either be json or csv.");
    }

    if (outputFile.equals("")) {
      writer = null;
    } else {
      try {
        writer = new FileWriter(outputFile);
      } catch (IOException e) {
        throw new StageException("Could not open the specified file.", e);
      }

    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (format.equals("csv")) {
      List<String> currentFields = fields.size() != 0 ? fields : new ArrayList<>(doc.asMap().keySet());
      StringBuilder builder = new StringBuilder();
      for (String field : currentFields) {
        builder.append(field).append(", ");
      }
      builder.delete(builder.length() - 2, builder.length() - 1);

      printCsv(doc, builder.append("\n"));
    } else {
      printJson(doc);
    }

    return null;
  }

  private void printCsv(Document doc, StringBuilder builder) throws StageException {
    List<String> currentFields = fields.size() != 0 ? fields : new ArrayList<>(doc.asMap().keySet());
    for (String field : currentFields) {
      List<String> values = doc.getStringList(field);
      Stream<String> valStream = values.stream().map((String value) -> StringUtils.abbreviate(value, maxFieldSize));
      builder.append(valStream.collect(Collectors.joining())).append(", ");
    }
    builder.delete(builder.length() - 2, builder.length() - 1);

    log.info(builder + "\n");

    if (writer != null) {
      try {
        writer.append(builder + "\n");
      } catch (IOException e) {
        throw new StageException("Could not write to the given file", e);
      }
    }
  }

  private void printJson(Document doc) throws StageException {
    List<String> currentFields = fields.size() != 0 ? fields : new ArrayList<>(doc.asMap().keySet());
    StringBuilder builder = new StringBuilder("{");
    for (String field : currentFields) {
      List<String> values = doc.getStringList(field);
      Stream<String> valStream = values.stream().map((String value) -> StringUtils.abbreviate(value, maxFieldSize));
      builder.append(field).append(":").append(valStream.collect(Collectors.joining())).append(",");
    }


    log.info(doc.toString());

    if (writer != null) {
      try {
        writer.append(builder + "\n");
      } catch (IOException e) {
        throw new StageException("Could not write to the given file", e);
      }
    }
  }
}
