package com.kmwllc.lucille.stage;

import java.net.URI;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * Copies values from a given set of source fields to a given set of destination
 * fields. If the same number of fields are supplied for both sources and
 * destinations, the fields will be copied from source_1 to dest_1 and source_2
 * to dest_2. If either source or dest has only one field value, and the other
 * has several, all of the fields will be copied to/from the same field.
 *
 * Config Parameters:
 *
 * - source (List&lt;String&gt;) : list of source field names - dest
 * (List&lt;String&gt;) : list of destination field names. You can either supply
 * the same number of source and destination fields for a 1-1 mapping of results
 * or supply one destination field for all of the source fields to be mapped
 * into. - update_mode (String, Optional) : Determines how writing will be
 * handling if the destination field is already populated. Can be 'overwrite',
 * 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class Nasuni extends Stage {

//  private final List<String> sourceFields;
//  private final List<String> destFields;
//  private final UpdateMode updateMode;

	private static final Logger log = LoggerFactory.getLogger(Nasuni.class);

	
	private transient S3Client s3;
	private transient S3Utilities s3Utilities;
	private String bucketName;

	public Nasuni(Config config) {
		super(config, new StageSpec()
				// .withRequiredProperties("file_path", "dest")
				// TODO - possibly get the aws creds from the FileConnector, but not sure that
				// is reachable from here
				.withRequiredProperties("region", "accessKeyId", "secretAccessKey", "pathToStorage"));
		// .withOptionalProperties("update_mode"));
//    this.sourceFields = config.getStringList("source");
//    this.destFields = config.getStringList("dest");
//    this.updateMode = UpdateMode.fromConfig(config);
	}

	@Override
	public void start() throws StageException {
//    StageUtils.validateFieldNumNotZero(sourceFields, "Copy Fields");
//    StageUtils.validateFieldNumNotZero(destFields, "Copy Fields");
//    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Copy Fields");
//      String bucketName = "your-bucket-name";
//      String objectKey = "your-object-key";

		AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(config.getString("accessKeyId"),
				config.getString("secretAccessKey"));
		Region region = Region.of(config.getString("region"));

		s3 = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(awsCredentials)).region(region)
				.build();
		s3Utilities = s3.utilities();
	    S3Uri parsedBucketUri = s3Utilities.parseUri(URI.create(config.getString("pathToStorage")));
	    bucketName = parsedBucketUri.bucket().orElse(null);
	    if (bucketName == null) {
	        throw new IllegalArgumentException(String.format("Invalid bucket URI %s", config.getString("pathToStorage")));
	    }
		
	}

	@Override
	public Iterator<Document> processDocument(Document doc) throws StageException {
		////////// begin ///////////////////////
		
		 S3Uri parsedObjectUri = s3Utilities.parseUri(URI.create(doc.getString("file_path")));
		    // String objectBucketName = parsedObjectUri.bucket().orElse(null);
		    String objectKey = parsedObjectUri.key().orElse(null);
//		    if (objectBucketName == null || objectKey == null) {
//		        throw new IllegalArgumentException("Invalid object URI");
//		    }		

		HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucketName)
				.key(objectKey).build();

		HeadObjectResponse headObjectResponse = s3.headObject(headObjectRequest);
		log.info("Content Type: " + headObjectResponse.contentType());
		log.info("Content Length: " + headObjectResponse.contentLength());
		log.info("ETag: " + headObjectResponse.eTag());
		String ntacl = headObjectResponse.metadata().get("security-ntacl");
		log.info(String.format("file_path: %s",  doc.getString("file_path")));
		log.info(String.format("security-ntacl: %s",  ntacl));
		if (ntacl != null) {
			doc.setField("security-ntacl", ntacl);
		}

		return null;
	}
}
