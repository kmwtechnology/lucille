package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.net.URI;
import java.util.List;

public class ParquetConnector extends AbstractConnector {

  private final String path;
  private final String idField;
  private final String fsUri;
  private final long limit;
  private final long start;

  private final String s3Key;
  private final String s3Secret;


  public ParquetConnector(Config config) {
    super(config);
    this.path = config.getString("path");
    this.idField = config.getString("id_field");
    this.fsUri = config.getString("fs_uri");


    this.s3Key = config.hasPath("s3_key") ? config.getString("s3_key") : null;
    this.s3Secret = config.hasPath("s3_secret") ? config.getString("s3_secret") : null;
    this.limit = config.hasPath("limit") ? config.getLong("limit") : -1;
    this.start = config.hasPath("start") ? config.getLong("start") : 0L;
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

    try (FileSystem fs = FileSystem.get(new URI(fsUri), conf)) {
      long count = 0L;
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new Path(path), true);
      while (statusIterator.hasNext()) {
        LocatedFileStatus status = statusIterator.next();
        //only process parquet files
        if (!status.getPath().getName().endsWith("parquet")) {
          continue;
        }
        if (limit > 0 && count >= limit) {
          break;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(status, conf))) {
          MessageType schema = reader.getFooter().getFileMetaData().getSchema();
          List<Type> fields = schema.getFields();
          PageReadStore pages;
          while ((pages = reader.readNextRowGroup()) != null) {
            if (limit > 0 && count >= limit) {
              break;
            }
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            for (int i = 0; i < rows; i++) {
              if (limit > 0 && count >= limit) {
                break;
              }

              SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
              String id = simpleGroup.getString(idField, 0);
              Document doc = Document.create(id);
              for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).getName().equals(idField)) {
                  continue;
                }
                if (fields.get(j).isPrimitive()) {
                  PrimitiveType.PrimitiveTypeName name = fields.get(j).asPrimitiveType().getPrimitiveTypeName();
                  switch (name) {
                    case BINARY:
                      doc.setField(fields.get(j).getName(), simpleGroup.getString(j, 0));
                      break;
                    case FLOAT:
                      doc.setField(fields.get(j).getName(), simpleGroup.getFloat(j, 0));
                      break;
                    case INT32:
                      doc.setField(fields.get(j).getName(), simpleGroup.getInteger(j, 0));
                      break;
                    case INT64:
                      doc.setField(fields.get(j).getName(), simpleGroup.getLong(j, 0));
                      break;
                    case DOUBLE:
                      doc.setField(fields.get(j).getName(), simpleGroup.getDouble(j, 0));
                      break;
                  }
                } else {
                  for (int k = 0; k < simpleGroup.getGroup(j, 0).getFieldRepetitionCount(0); k++) {
                    Group group = simpleGroup.getGroup(j, 0).getGroup(0, k);
                    Type type = group.getType().getType(0);
                    if (type.isPrimitive()) {
                      PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
                      switch (name) {
                        case BINARY:
                          doc.addToField(fields.get(j).getName(), group.getString(0, 0));
                          break;
                        case FLOAT:
                          doc.addToField(fields.get(j).getName(), group.getFloat(0, 0));
                          break;
                        case INT32:
                          doc.addToField(fields.get(j).getName(), group.getInteger(0, 0));
                          break;
                        case INT64:
                          doc.addToField(fields.get(j).getName(), group.getLong(0, 0));
                          break;
                        case DOUBLE:
                          doc.addToField(fields.get(j).getName(), group.getDouble(0, 0));
                          break;
                      }
                    }
                  }
                }
              }
              count++;
              if (count > start) {
                publisher.publish(doc);
              }
            }
          }
        } catch (Exception e) {
          throw new ConnectorException("Problem running the connector.", e);
        }
      }
    } catch (Exception e) {
      throw new ConnectorException("Problem running the ParquetConnector", e);
    }

  }
}
