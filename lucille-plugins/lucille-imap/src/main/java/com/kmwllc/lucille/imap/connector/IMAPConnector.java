package com.kmwllc.lucille.imap.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import jakarta.mail.Address;
import jakarta.mail.AuthenticationFailedException;
import jakarta.mail.BodyPart;
import jakarta.mail.FetchProfile;
import jakarta.mail.Folder;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.internet.MailDateFormat;
import jakarta.mail.internet.MimeMultipart;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.eclipse.angus.mail.imap.IMAPFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects to an IMAP mailbox, crawls one or more folders, and publishes a Document for each email message
 * encountered so it can be indexed downstream.
 *
 * <p>Each Document is given an ID derived from the email's <code>Message-ID</code> header (falling back to a
 * random UUID if the header is absent), prefixed with the connector's <code>docIdPrefix</code>. Every email
 * header is copied onto the Document using a cleaned (lower-cased, underscore-separated) field name. In addition,
 * the following fields are populated where available: <code>folder</code>, <code>subject</code>, <code>from</code>,
 * <code>to</code>, <code>cc</code>, <code>bcc</code> (multivalued), <code>reply_to</code> (multivalued),
 * <code>sent_date</code>, <code>received_date</code>, <code>size</code>, <code>text</code> (plain text body),
 * and <code>html</code> (HTML body).
 *
 * <p>The mailbox is always opened read-only so messages are never modified or deleted.
 *
 * <p>Config Parameters:
 * <ul>
 *   <li>host (String): The hostname of the IMAP server (for example, <code>imap.gmail.com</code>).</li>
 *   <li>username (String): The username / email address used to authenticate.</li>
 *   <li>password (String): The password (or app password) used to authenticate.</li>
 *   <li>port (Integer, Optional): The port to connect to. Defaults to the protocol default (993 for IMAPS, 143 for IMAP).</li>
 *   <li>folder (String, Optional): The name of the folder to crawl. Defaults to <code>INBOX</code>.</li>
 *   <li>useSSL (Boolean, Optional): Whether to connect using IMAPS (SSL/TLS). Defaults to <code>true</code>.</li>
 *   <li>recurse (Boolean, Optional): Whether to also crawl sub-folders of the configured folder. Defaults to <code>false</code>.</li>
 *   <li>fetchBatchSize (Integer, Optional): How many messages to bulk-prefetch per server round-trip while crawling.
 *   Larger values reduce network round-trips (faster) at the cost of more memory per batch. Defaults to <code>100</code>.</li>
 *   <li>prefetchBody (Boolean, Optional): Whether to bulk-prefetch full message bodies along with metadata. Keeping this
 *   <code>true</code> is dramatically faster for body-heavy crawls but uses more memory per batch; set to <code>false</code>
 *   to prefetch only metadata and fetch bodies lazily. Defaults to <code>true</code>. Only applies to IMAP folders.</li>
 *   <li>excludeHeaderPrefixes (List&lt;String&gt;, Optional): Email headers whose (cleaned, lower-cased, underscore-separated)
 *   name starts with any of these prefixes are NOT copied onto the Document. This prevents the unbounded, noisy header
 *   families (especially the <code>X-*</code> headers added by mail infrastructure / ESPs) from causing a field-mapping
 *   explosion in the downstream index. Defaults to <code>["x_"]</code>. Set to <code>[]</code> to copy every header.</li>
 * </ul>
 */
public class IMAPConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("host", "username", "password")
      .optionalString("folder")
      .optionalNumber("port", "fetchBatchSize")
      .optionalBoolean("useSSL", "recurse", "prefetchBody")
      .optionalList("excludeHeaderPrefixes", new TypeReference<List<String>>(){})
      .build();

  private static final Logger log = LoggerFactory.getLogger(IMAPConnector.class);

  private static final String MESSAGE_ID_HEADER = "message_id";

  // Number of messages to bulk-prefetch per server round-trip when crawling.
  private static final int DEFAULT_FETCH_BATCH_SIZE = 100;

  // By default, skip the noisy "X-*" header family (added by mail infra / ESPs) to avoid an unbounded number of
  // unique fields exploding the downstream index mapping. Matched against the cleaned (lower-cased, underscore) name.
  private static final List<String> DEFAULT_EXCLUDE_HEADER_PREFIXES = List.of("x_");

  private final String host;
  private final String username;
  private final String password;
  private final int port;
  private final String folderName;
  private final boolean useSSL;
  private final boolean recurse;
  private final String protocol;
  private final int fetchBatchSize;
  private final boolean prefetchBody;
  private final List<String> excludeHeaderPrefixes;

  private Store store;
  // True when the Store was supplied externally (e.g. for testing); in that case we don't open/close it ourselves.
  private boolean externalStore;

  public IMAPConnector(Config config) {
    super(config);
    this.host = config.getString("host");
    this.username = config.getString("username");
    this.password = config.getString("password");
    this.port = config.hasPath("port") ? config.getInt("port") : -1;
    this.folderName = config.hasPath("folder") ? config.getString("folder") : "INBOX";
    this.useSSL = config.hasPath("useSSL") ? config.getBoolean("useSSL") : true;
    this.recurse = config.hasPath("recurse") ? config.getBoolean("recurse") : false;
    this.protocol = useSSL ? "imaps" : "imap";
    int configuredBatchSize = config.hasPath("fetchBatchSize") ? config.getInt("fetchBatchSize") : DEFAULT_FETCH_BATCH_SIZE;
    this.fetchBatchSize = Math.max(1, configuredBatchSize);
    this.prefetchBody = config.hasPath("prefetchBody") ? config.getBoolean("prefetchBody") : true;
    this.excludeHeaderPrefixes = config.hasPath("excludeHeaderPrefixes")
        ? config.getStringList("excludeHeaderPrefixes").stream()
            .map(IMAPConnector::cleanFieldName)
            .filter(prefix -> !prefix.isEmpty())
            .toList()
        : DEFAULT_EXCLUDE_HEADER_PREFIXES;
    this.externalStore = false;
  }

  /**
   * Test / advanced constructor that allows an already-connected {@link Store} to be injected, bypassing the
   * connection logic. The provided Store will not be opened or closed by this Connector.
   *
   * @param config the connector configuration
   * @param store an already-connected mail Store to read from
   */
  public IMAPConnector(Config config, Store store) {
    this(config);
    this.store = store;
    this.externalStore = true;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    boolean opened = false;
    try {
      if (store == null) {
        store = connect();
        opened = true;
      }

      Folder folder = store.getFolder(folderName);
      openFolder(folder);

      int count = processFolder(folder, publisher);

      if (recurse) {
        for (Folder subFolder : folder.list()) {
          openFolder(subFolder);
          count += processFolder(subFolder, publisher);
        }
      }

      log.info("IMAPConnector published {} email message(s) from {}", count, host);
      setMessage("Published " + count + " email message(s).");
    } catch (MessagingException e) {
      throw new ConnectorException("Error crawling IMAP mailbox on host " + host, e);
    } finally {
      if (opened) {
        closeStore();
      }
    }
  }

  private Store connect() throws ConnectorException {
    Properties props = new Properties();
    props.setProperty("mail.store.protocol", protocol);
    Session session = Session.getInstance(props, null);

    try {
      Store newStore = session.getStore(protocol);
      if (port > 0) {
        newStore.connect(host, port, username, password);
      } else {
        newStore.connect(host, username, password);
      }
      log.info("Connected to IMAP store on host {}", host);
      return newStore;
    } catch (AuthenticationFailedException e) {
      String guidance = "Authentication failed for user '" + username + "' on IMAP server " + host + ". "
          + "Verify the username and password are correct. Note that Gmail / Google Workspace no longer accept "
          + "regular account passwords over IMAP - you must use a 16-character App Password "
          + "(https://myaccount.google.com/apppasswords), enter it without spaces, and have IMAP enabled.";
      throw new ConnectorException(guidance, e);
    } catch (MessagingException e) {
      throw new ConnectorException("Failed to connect to IMAP server " + host, e);
    }
  }

  private void openFolder(Folder folder) throws ConnectorException {
    try {
      if (!folder.isOpen()) {
        // Read only - we never want to mutate or delete the user's mail.
        folder.open(Folder.READ_ONLY);
      }
    } catch (MessagingException e) {
      throw new ConnectorException("Unable to open folder " + safeFolderName(folder), e);
    }
  }

  private int processFolder(Folder folder, Publisher publisher) throws ConnectorException {
    String currentFolderName = safeFolderName(folder);
    log.info("Processing folder {}", currentFolderName);

    int numDocs = 0;
    try {
      Message[] messages = folder.getMessages();

      // Process in batches, bulk-prefetching each batch in a single server round-trip. Without this, every
      // accessor (headers, envelope, body, etc.) on every message triggers its own IMAP FETCH, making large
      // crawls extremely slow due to per-message network latency.
      for (int start = 0; start < messages.length; start += fetchBatchSize) {
        int end = Math.min(start + fetchBatchSize, messages.length);
        Message[] batch = Arrays.copyOfRange(messages, start, end);

        prefetch(folder, batch);

        for (Message message : batch) {
          try {
            Document doc = processMessage(message);
            doc.setField("folder", currentFolderName);
            publisher.publish(doc);
            numDocs++;
          } catch (Exception e) {
            log.warn("Failed to process a message in folder {}, skipping it.", currentFolderName, e);
          }
        }
      }
    } catch (MessagingException e) {
      throw new ConnectorException("Error reading messages from folder " + currentFolderName, e);
    }

    return numDocs;
  }

  /**
   * Bulk-prefetches the data for a batch of messages in a single (or few) IMAP FETCH command(s), so that the
   * subsequent per-message accessors in {@link #processMessage(Message)} are served from a local cache instead
   * of each triggering its own server round-trip. This is the primary performance optimization for crawling.
   */
  private void prefetch(Folder folder, Message[] batch) {
    if (batch.length == 0) {
      return;
    }

    try {
      FetchProfile fetchProfile = new FetchProfile();
      // Portable items: structured envelope (from/to/cc/subject/sent-date) and body structure.
      fetchProfile.add(FetchProfile.Item.ENVELOPE);
      fetchProfile.add(FetchProfile.Item.CONTENT_INFO);

      // IMAP-specific items let us prefetch everything else we read so nothing falls back to a per-message fetch.
      if (folder instanceof IMAPFolder) {
        fetchProfile.add(IMAPFolder.FetchProfileItem.HEADERS);
        fetchProfile.add(IMAPFolder.FetchProfileItem.INTERNALDATE);
        fetchProfile.add(IMAPFolder.FetchProfileItem.SIZE);
        if (prefetchBody) {
          // Prefetches the full raw message (headers + body) for the whole batch in one round-trip.
          fetchProfile.add(IMAPFolder.FetchProfileItem.MESSAGE);
        }
      }

      folder.fetch(batch, fetchProfile);
    } catch (MessagingException e) {
      // Prefetch is an optimization; if it fails, fall back to lazy per-message fetching.
      log.warn("Bulk prefetch failed for a batch in folder {}; continuing with per-message fetching.",
          safeFolderName(folder), e);
    }
  }

  // Package-private to allow direct unit testing with real Message instances (no live server / mocks needed).
  Document processMessage(Message message) throws Exception {
    // Buffer the headers so we can derive the Document id (from the Message-ID) before creating the Document.
    List<Header> headerList = new ArrayList<>();
    Enumeration<Header> headers = message.getAllHeaders();
    String messageId = null;
    while (headers.hasMoreElements()) {
      Header header = headers.nextElement();
      headerList.add(header);
      if (MESSAGE_ID_HEADER.equals(cleanFieldName(header.getName()))) {
        messageId = header.getValue();
      }
    }

    String rawId = (messageId != null && !messageId.isBlank()) ? messageId : UUID.randomUUID().toString();
    Document doc = Document.create(createDocId(rawId));

    for (Header header : headerList) {
      String fieldName = cleanFieldName(header.getName());

      // Don't let header names collide with Lucille's reserved fields.
      if (Document.RESERVED_FIELDS.contains(fieldName)) {
        continue;
      }

      // Skip noisy header families (e.g. X-*) to avoid an unbounded number of unique fields downstream.
      if (isExcludedHeader(fieldName)) {
        continue;
      }

      doc.addToField(fieldName, header.getValue());
    }

    addRecipients(doc, message, "from", message.getFrom());
    addRecipients(doc, message, "to", message.getRecipients(Message.RecipientType.TO));
    addRecipients(doc, message, "cc", message.getRecipients(Message.RecipientType.CC));
    addRecipients(doc, message, "bcc", message.getRecipients(Message.RecipientType.BCC));
    addRecipients(doc, message, "reply_to", message.getReplyTo());

    String subject = message.getSubject();
    if (subject != null) {
      doc.setField("subject", subject);
    }

    setSentDate(doc, message);

    Date receivedDate = message.getReceivedDate();
    if (receivedDate != null) {
      doc.setField("received_date", receivedDate.toInstant());
    }

    parseContent(message, doc);

    doc.setField("size", message.getSize());

    return doc;
  }

  private void setSentDate(Document doc, Message message) throws MessagingException {
    Date sentDate = message.getSentDate();
    if (sentDate != null) {
      doc.setField("sent_date", sentDate.toInstant());
      return;
    }

    // Fall back to parsing the raw Date header if the parsed sent date is unavailable.
    if (doc.has("date")) {
      try {
        Date parsed = new MailDateFormat().parse(doc.getStringList("date").get(0));
        if (parsed != null) {
          doc.setField("sent_date", parsed.toInstant());
        }
      } catch (ParseException e) {
        log.warn("Unable to parse Date header into a sent_date.", e);
      }
    }
  }

  private void addRecipients(Document doc, Message message, String fieldName, Address[] addresses) {
    if (addresses == null) {
      return;
    }
    for (Address address : addresses) {
      if (address != null) {
        doc.addToField(fieldName, address.toString());
      }
    }
  }

  private void parseContent(Part part, Document doc) throws Exception {
    Object content;
    try {
      content = part.getContent();
    } catch (Exception e) {
      log.warn("Unable to read content for a message part, skipping it.", e);
      return;
    }

    if (part.isMimeType("text/plain")) {
      doc.addToField("text", String.valueOf(content));
    } else if (part.isMimeType("text/html")) {
      doc.addToField("html", String.valueOf(content));
    } else if (content instanceof MimeMultipart) {
      MimeMultipart multipart = (MimeMultipart) content;
      for (int i = 0; i < multipart.getCount(); i++) {
        BodyPart bodyPart = multipart.getBodyPart(i);
        parseContent(bodyPart, doc);
      }
    } else if (content instanceof String) {
      doc.addToField("text", (String) content);
    } else {
      log.debug("Skipping unhandled content type {}", part.getContentType());
    }
  }

  private boolean isExcludedHeader(String cleanedName) {
    for (String prefix : excludeHeaderPrefixes) {
      if (cleanedName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static String cleanFieldName(String name) {
    return name.trim().toLowerCase().replaceAll("[ -]", "_");
  }

  private static String safeFolderName(Folder folder) {
    try {
      return folder.getName();
    } catch (Exception e) {
      return "unknown";
    }
  }

  @Override
  public void close() throws ConnectorException {
    if (!externalStore) {
      closeStore();
    }
  }

  private void closeStore() {
    if (store != null && store.isConnected()) {
      try {
        store.close();
      } catch (MessagingException e) {
        log.warn("Error closing IMAP store.", e);
      }
    }
    if (!externalStore) {
      store = null;
    }
  }
}
