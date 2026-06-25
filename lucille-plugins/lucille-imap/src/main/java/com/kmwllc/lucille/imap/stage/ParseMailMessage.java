package com.kmwllc.lucille.imap.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.imap.EmailMessageParser;
import com.typesafe.config.Config;
import jakarta.mail.Message;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the raw RFC822 email bytes placed on a Document by {@link com.kmwllc.lucille.imap.connector.IMAPConnector}
 * and populates the standard email fields (headers, recipients, subject, dates, plain-text / HTML bodies, size).
 *
 * <p>The connector assigns the Document id from the Message-ID header and publishes the raw bytes so this stage can run
 * concurrently in the worker pool while the connector continues fetching the next batch over IMAP.
 *
 * <p>Config Parameters:
 * <ul>
 *   <li>rawMessageField (String, Optional): Name of the field holding the raw RFC822 bytes. Defaults to
 *   <code>imap_raw_message</code>.</li>
 *   <li>excludeHeaderPrefixes (List&lt;String&gt;, Optional): Email headers whose (cleaned, lower-cased,
 *   underscore-separated) name starts with any of these prefixes are NOT copied onto the Document. Defaults to
 *   <code>["x_"]</code>. Set to <code>[]</code> to copy every header.</li>
 *   <li>deleteRawMessageField (Boolean, Optional): Whether to remove the raw message field from the Document after
 *   parsing so the large byte array is not indexed downstream. Defaults to <code>true</code>.</li>
 * </ul>
 */
public class ParseMailMessage extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("rawMessageField")
      .optionalBoolean("deleteRawMessageField")
      .optionalList("excludeHeaderPrefixes", new TypeReference<List<String>>() {})
      .build();

  private static final Logger log = LoggerFactory.getLogger(ParseMailMessage.class);

  private final String rawMessageField;
  private final boolean deleteRawMessageField;
  private final List<String> excludeHeaderPrefixes;

  public ParseMailMessage(Config config) {
    super(config);
    this.rawMessageField = config.hasPath("rawMessageField")
        ? config.getString("rawMessageField")
        : EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD;
    this.deleteRawMessageField = ConfigUtils.getOrDefault(config, "deleteRawMessageField", true);
    this.excludeHeaderPrefixes = config.hasPath("excludeHeaderPrefixes")
        ? EmailMessageParser.normalizeExcludePrefixes(config.getStringList("excludeHeaderPrefixes"))
        : EmailMessageParser.normalizeExcludePrefixes(List.of("x_"));
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(rawMessageField)) {
      log.warn("Document {} has no raw message field {}; skipping parse.", doc.getId(), rawMessageField);
      return null;
    }

    byte[] raw = doc.getBytes(rawMessageField);
    if (raw == null || raw.length == 0) {
      log.warn("Document {} has an empty raw message field {}; skipping parse.", doc.getId(), rawMessageField);
      return null;
    }

    try {
      Message message = EmailMessageParser.toMimeMessage(raw);
      EmailMessageParser.populateDocument(message, doc, excludeHeaderPrefixes);
      if (deleteRawMessageField) {
        doc.removeField(rawMessageField);
      }
    } catch (Exception e) {
      throw new StageException("Failed to parse email message for document " + doc.getId(), e);
    }

    return null;
  }
}
