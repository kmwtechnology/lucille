package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.List;

/**
 * Logs all received documents in JSON format and/or writes them to a designated file.
 * <p>
 * Config Parameters -
 * <p>
 * - shouldLog (Boolean, Optional) : Whether to log the document in JSON format at INFO level. Defaults to true.
 * - outputFile (String, Optional) : A file to append the documents to (will be created if it doesn't already exist).
 * - excludeFields (String list, Optional) : A list of fields to exclude from the output.
 * - overwriteFile (Boolean, Optional) : Whether the output file should overwritten if it already exists. Defaults to true.
 */
public class Print extends Stage {

  private final String outputFile;
  private boolean shouldLog;
  private boolean overwriteFile;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Writer writer = null;
  private List<String> excludeFields;

  public Print(Config config) {
    super(new StageBuilder(config)
      .withOptionalProperties("shouldLog", "outputFile", "overwriteFile", "excludeFields"));
    this.outputFile = config.hasPath("outputFile") ? config.getString("outputFile") : null;
    this.shouldLog = config.hasPath("shouldLog") ? config.getBoolean("shouldLog") : true;
    this.excludeFields = config.hasPath("excludeFields") ? config.getStringList("excludeFields") : null;
    this.overwriteFile = config.hasPath("overwriteFile") ? config.getBoolean("overwriteFile") : true;
  }

  public void start() throws StageException {
    if (outputFile!=null) {
      try {
        writer = new BufferedWriter(new FileWriter(outputFile, !overwriteFile));
      } catch (IOException e) {
        throw new StageException("Could not open the specified file.", e);
      }
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (excludeFields!=null) {
      doc = doc.deepCopy();
      for (String field : excludeFields) {
        if (Document.RUNID_FIELD.equals(field)) {
          doc.clearRunId();
        } else {
          doc.removeField(field);
        }
      }
    }

    if (shouldLog) {
      log.info(doc.toString());
    }

    if (writer!=null) {
      try {
        writer.append(doc.toString() + "\n");
      } catch (IOException e) {
        throw new StageException("Could not write to the given file", e);
      }
    }

    return null;
  }

  @Override
  public void stop() throws StageException {
    if (writer!=null) {
      try {
        writer.close();
      } catch (IOException e) {
        throw new StageException("Error closing writer.", e);
      }
    }
  }

}
