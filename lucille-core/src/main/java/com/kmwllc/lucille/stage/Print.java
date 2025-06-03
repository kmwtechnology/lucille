package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Logs all received documents in JSON format and/or writes them to a designated file.
 * <p> Config Parameters -
 * <ul>
 *   <li>shouldLog (Boolean, Optional): Whether to log the document in JSON format at INFO level. Defaults to true.</li>
 *   <li>outputFile (String, Optional): A file to append the documents to (will be created if it doesn't already exist).</li>
 *   <li>excludeFields (List&lt;String&gt;, Optional): A list of fields to exclude from the output.</li>
 *   <li>overwriteFile (Boolean, Optional): Whether the output file's contents should be overwritten if they already exist. Defaults to true.</li>
 *   <li>appendThreadName (Boolean, Optional): Whether the current thread's name should be appended to the outputFile's filename, keeping the results from individual threads separate. Has no effect if no outputFile is provided. Defaults to true.</li>
 * </ul>
 *
 * <p> <b>Note:</b> If you have multiple worker threads, it is highly recommended that you keep <code>appendThreadName</code> enabled
 * to ensure thread-safe file writes and prevent data corruption - <i>especially</i> if <code>overwriteFile</code> is enabled.
 */
public class Print extends Stage {

  private static final Logger log = LoggerFactory.getLogger(Print.class);

  private final String outputFilePath;
  private final boolean shouldLog;
  private final boolean overwriteFile;
  private final boolean appendThreadName;
  private final List<String> excludeFields;

  private BufferedWriter writer;

  public Print(Config config) {
    super(config, Spec.stage().withOptionalProperties("shouldLog", "outputFile",
        "overwriteFile", "excludeFields", "appendThreadName"));

    this.outputFilePath = config.hasPath("outputFile") ? config.getString("outputFile") : null;
    this.shouldLog = config.hasPath("shouldLog") ? config.getBoolean("shouldLog") : true;
    this.excludeFields = config.hasPath("excludeFields") ? config.getStringList("excludeFields") : null;
    this.overwriteFile = config.hasPath("overwriteFile") ? config.getBoolean("overwriteFile") : true;
    this.appendThreadName = ConfigUtils.getOrDefault(config, "appendThreadName", true);

    // if appendThreadName is *explicitly* set to true in the config, but no output file, warn it has no effect.
    if ((config.hasPath("appendThreadName") && config.getBoolean("appendThreadName")) && outputFilePath == null) {
      log.warn("appendThreadName was set to true in Print Config, but no outputFile was specified. appendThreadName has no effect.");
    }
  }

  // we always create the writer in processDocument - not in start(). This allows us to get the name of the
  // Worker thread that will actually be using the Stage, in case appendThreadName is true. (Since Stages/Pipelines are always
  // constructed and start()ed in the main thread).

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // create the file writer, if needed and not already created. this allows us to get the name of the Worker thread that
    // will actually be using the stage, in case appendThreadName is true.
    if (outputFilePath != null && writer == null) {
      try {
        if (appendThreadName) {
          String appendedOutputFilePath = FileUtils.appendStringToFileName(outputFilePath, Thread.currentThread().getName());
          writer = new BufferedWriter(new FileWriter(appendedOutputFilePath, !overwriteFile));
        } else {
          writer = new BufferedWriter(new FileWriter(outputFilePath, !overwriteFile));
        }
      } catch (IOException e) {
        throw new StageException("Couldn't initialize FileWriter / BufferedWriter.", e);
      }
    }

    if (excludeFields != null) {
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

    if (writer != null) {
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
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        throw new StageException("Couldn't close a BufferedWriter.", e);
      }
    }
  }
}
