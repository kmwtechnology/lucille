package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

public class ParquetConnector extends AbstractConnector {

  private final String path;
  private final String idField;
  private final String fsUri;
  private final long limit;

  private final String s3Key;
  private final String s3Secret;
  
  private final String seedUriFile;

  // non final
  private long start;
  private long count = 0L;


  public ParquetConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("path", "id_field", "fs_uri")
        .withOptionalProperties("s3_key", "s3_secret", "limit", "start"));
    this.path = config.getString("path");
    this.idField = config.getString("id_field");
    this.fsUri = config.getString("fs_uri");

    this.s3Key = config.hasPath("s3_key") ? config.getString("s3_key") : null;
    this.s3Secret = config.hasPath("s3_secret") ? config.getString("s3_secret") : null;
    this.limit = config.hasPath("limit") ? config.getLong("limit") : -1;
    this.start = config.hasPath("start") ? config.getLong("start") : 0L;
    this.seedUriFile = config.hasPath("seed_uri_file") ? config.getString("seed_uri_file") : null;
  }

  private boolean limitNotReached() {
    return limit < 0 || count < limit;
  }

  private boolean canSkipAndUpdateStart(long n) {
    if (start > n) {
      start -= n;
      return true;
    }
    return false;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    Configuration conf = new Configuration();
    if (s3Key != null && s3Secret != null) {
      conf.set("fs.s3a.access.key", s3Key);
      conf.set("fs.s3a.secret.key", s3Secret);
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.setBoolean("fs.s3a.path.style.access", true);
      conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
    }

    TreeSet<String> seedUris = null;
    // If there's a seed uri file.. we should open it and iterate that instead.
    if (seedUriFile != null) {
      seedUris = readLines(seedUriFile);
      // TODO: process these instead
      System.err.println("SEED URI FILE LOADED : " + seedUris.size());
    }
    
    
    // TODO: expose a location for this file.
    String incrementalFile = getName() + "_incremental.dat";
    // First read the incremental file in.. 
    HashSet<String> alreadyProcessed = readAlreadyProcessedFile(incrementalFile);

    FileWriter processLog = null;
    try {
      processLog = new FileWriter(incrementalFile, true);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    BufferedWriter bw = new BufferedWriter(processLog);

    try (FileSystem fs = FileSystem.get(new URI(fsUri), conf)) {
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new Path(path), true);
      while (limitNotReached() && statusIterator.hasNext()) {
        LocatedFileStatus status = statusIterator.next();
        //only process parquet files
        if (!status.getPath().getName().endsWith("parquet")) {
          continue;
        }

        String fileUri = status.getPath().toUri().toString();
        System.err.println("Scanning file :" + fileUri);
        
        //        bw.write(fileUri + "\n");
        //        bw.flush();
        //        if (true)
        //          continue;
        if (alreadyProcessed.contains(fileUri)) {
          // We have already processed this file.. continue to the next one.
          System.err.println("Already processed file, skipping: " + fileUri);
          continue;
        }

        // now if we have a seedUriFile we should check if our uri is in there.
        if (seedUris != null && !seedUris.contains(fileUri)) {
          // skip this file.
          System.err.println("Skipping file, it's not in the list. " + fileUri);
          continue;
        }
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(status, conf))) {

          // check if we can skip this file
          if (canSkipAndUpdateStart(reader.getRecordCount())) {
            continue;
          }

          MessageType schema = reader.getFooter().getFileMetaData().getSchema();
          List<Type> fields = schema.getFields();
          PageReadStore pages;
          while (limitNotReached() && (pages = reader.readNextRowGroup()) != null) {
            
            // check if we can skip this row group
            long nRows = pages.getRowCount();
            if (canSkipAndUpdateStart(nRows)) {
              continue;
            }

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            // at this point start is either 0 or < nRows, if later will update start to 0 after the loop
            while (limitNotReached() && nRows-- > 0) {

              // read record regardless of the st art parameter
              SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
              if (canSkipAndUpdateStart(1)) {
                continue;
              }

              String id = simpleGroup.getString(idField, 0);
              Document doc = simpleGroupToDocument(fields, simpleGroup, id);
              publisher.publish(doc);
              count++;
            }
          }
        } catch (Exception e) {
          throw new ConnectorException("Problem running the connector.", e);
        }
        // If we got here, the current file has been procssed.
        bw.write(fileUri + "\n");
        bw.flush();
      }
    } catch (Exception e) {
      throw new ConnectorException("Problem running the ParquetConnector", e);
    }
    
    try {
      bw.close();
    } catch (IOException e) {
      //
      e.printStackTrace();
    }
  }

  private Document simpleGroupToDocument(List<Type> fields, SimpleGroup simpleGroup, String id) {
    Document doc = Document.create(id);
    for (int j = 0; j < fields.size(); j++) {

      Type field = fields.get(j);
      String fieldName = field.getName();
      if (fieldName.equals(idField)) {
        continue;
      }
      if (field.isPrimitive()) {
        setDocField(doc, field, simpleGroup, j);
      } else {
        for (int k = 0; k < simpleGroup.getGroup(j, 0).getFieldRepetitionCount(0); k++) {
          Group group = simpleGroup.getGroup(j, 0).getGroup(0, k);
          Type type = group.getType().getType(0);
          if (type.isPrimitive()) {
            addToField(doc, fieldName, type, group);
          }
        }
      }
    }
    return doc;
  }

  private static TreeSet<String> readLines(String seedUriFilename) {
    TreeSet<String> lines = new TreeSet<String>();
    // ArrayList<String> lines = new ArrayList<String>();
    FileReader reader;
    try {
      reader = new FileReader(seedUriFilename);
      BufferedReader br = new BufferedReader(reader);
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) {
          continue;
        }
        lines.add(line);
      }
      br.close();
      reader.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return lines;
  }
  
  private static HashSet<String> readAlreadyProcessedFile(String incrementalFile) {
    HashSet<String> alreadyProcessed = new HashSet<String>();
    FileReader reader;
    try {
      reader = new FileReader(incrementalFile);
      BufferedReader br = new BufferedReader(reader);
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) {
          continue;
        }
        alreadyProcessed.add(line);
      }
      br.close();
      reader.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return alreadyProcessed;
  }

  private static void setDocField(Document doc, Type field, SimpleGroup simpleGroup, int j) {

    // check if we have a field value before setting it.
    if (simpleGroup.getFieldRepetitionCount(j) == 0) {
      return;
    }

    String fieldName = field.getName();

    switch (field.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.setField(fieldName, simpleGroup.getString(j, 0));
        break;
      case FLOAT:
        doc.setField(fieldName, simpleGroup.getFloat(j, 0));
        break;
      case INT32:
        doc.setField(fieldName, simpleGroup.getInteger(j, 0));
        break;
      case INT64:
        doc.setField(fieldName, simpleGroup.getLong(j, 0));
        break;
      case DOUBLE:
        doc.setField(fieldName, simpleGroup.getDouble(j, 0));
        break;
      // todo consider adding a default case
    }
  }

  private static void addToField(Document doc, String fieldName, Type type, Group group) {
    switch (type.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.addToField(fieldName, group.getString(0, 0));
        break;
      case FLOAT:
        doc.addToField(fieldName, group.getFloat(0, 0));
        break;
      case INT32:
        doc.addToField(fieldName, group.getInteger(0, 0));
        break;
      case INT64:
        doc.addToField(fieldName, group.getLong(0, 0));
        break;
      case DOUBLE:
        doc.addToField(fieldName, group.getDouble(0, 0));
        break;
      // todo consider adding a default case
    }
  }
}
