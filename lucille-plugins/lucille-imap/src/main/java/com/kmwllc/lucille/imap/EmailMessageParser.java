package com.kmwllc.lucille.imap;

import com.kmwllc.lucille.core.Document;
import jakarta.mail.Address;
import jakarta.mail.BodyPart;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MailDateFormat;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.internet.AddressException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses RFC822 email messages into Lucille {@link Document} fields. Used by {@link
 * com.kmwllc.lucille.imap.stage.ParseMailMessage} after the {@link com.kmwllc.lucille.imap.connector.IMAPConnector}
 * has published the raw bytes.
 */
public final class EmailMessageParser {

  private static final Logger log = LoggerFactory.getLogger(EmailMessageParser.class);

  /** Default field name for the raw RFC822 bytes emitted by {@link com.kmwllc.lucille.imap.connector.IMAPConnector}. */
  public static final String DEFAULT_RAW_MESSAGE_FIELD = "imap_raw_message";

  private static final List<String> DEFAULT_EXCLUDE_HEADER_PREFIXES = List.of("x_");
  private static final Session SESSION = Session.getInstance(new Properties());

  // Recipient headers are populated from structured Address objects (multi-valued, one entry per address) rather
  // than copied from the raw header lines, which are often a single comma-separated string.
  private static final Set<String> RECIPIENT_HEADER_FIELDS = Set.of("from", "to", "cc", "bcc", "reply_to");
  private static final Set<String> EMAIL_ADDRESS_FIELDS = Set.of("from", "to", "cc", "bcc");

  private EmailMessageParser() {
  }

  /**
   * Returns the value of the Message-ID header, or {@code null} if absent / blank.
   */
  public static String extractMessageId(Message message) throws MessagingException {
    String[] ids = message.getHeader("Message-ID");
    if (ids != null && ids.length > 0 && ids[0] != null && !ids[0].isBlank()) {
      return ids[0];
    }
    return null;
  }

  /**
   * Derives the raw document id (before {@code docIdPrefix}) from a message's Message-ID header, generating a UUID
   * when the header is absent.
   */
  public static String deriveRawDocumentId(Message message) throws MessagingException {
    String messageId = extractMessageId(message);
    return (messageId != null && !messageId.isBlank()) ? messageId : UUID.randomUUID().toString();
  }

  /**
   * Serializes a message to its full RFC822 byte representation.
   */
  public static byte[] toRawBytes(Message message) throws IOException, MessagingException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    message.writeTo(out);
    return out.toByteArray();
  }

  /**
   * Reconstructs a {@link MimeMessage} from raw RFC822 bytes.
   */
  public static MimeMessage toMimeMessage(byte[] raw) throws MessagingException {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw));
  }

  /**
   * Populates a Lucille document (which must already have its id set) with the parsed email fields using the default
   * X-* header exclusion.
   */
  public static void populateDocument(Message message, Document doc) throws Exception {
    populateDocument(message, doc, DEFAULT_EXCLUDE_HEADER_PREFIXES);
  }

  /**
   * Populates a Lucille document (which must already have its id set) with the parsed email fields.
   */
  public static void populateDocument(Message message, Document doc, List<String> excludeHeaderPrefixes)
      throws Exception {
    List<Header> headerList = new ArrayList<>();
    Enumeration<Header> headers = message.getAllHeaders();
    while (headers.hasMoreElements()) {
      headerList.add(headers.nextElement());
    }

    for (Header header : headerList) {
      String fieldName = cleanFieldName(header.getName());

      if (Document.RESERVED_FIELDS.contains(fieldName)) {
        continue;
      }

      if (isExcludedHeader(fieldName, excludeHeaderPrefixes)) {
        continue;
      }

      if (isRecipientHeader(fieldName)) {
        continue;
      }

      doc.addToField(fieldName, header.getValue());
    }

    clearRecipientFields(doc);

    Set<String> emailDomains = new LinkedHashSet<>();
    addRecipientFields(doc, "from", expandToIndividualAddresses(message.getFrom()), emailDomains);
    addRecipientFields(doc, "to", expandToIndividualAddresses(
        message.getRecipients(Message.RecipientType.TO)), emailDomains);
    addRecipientFields(doc, "cc", expandToIndividualAddresses(
        message.getRecipients(Message.RecipientType.CC)), emailDomains);
    addRecipientFields(doc, "bcc", expandToIndividualAddresses(
        message.getRecipients(Message.RecipientType.BCC)), emailDomains);
    addRecipients(doc, "reply_to", expandToIndividualAddresses(message.getReplyTo()));

    for (String domain : emailDomains) {
      doc.addToField("email_domains", domain);
    }

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
  }

  private static void setSentDate(Document doc, Message message) throws MessagingException {
    Date sentDate = message.getSentDate();
    if (sentDate != null) {
      doc.setField("sent_date", sentDate.toInstant());
      return;
    }

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

  private static void clearRecipientFields(Document doc) {
    for (String field : List.of(
        "from", "to", "cc", "bcc", "reply_to",
        "from_raw", "to_raw", "cc_raw", "bcc_raw", "reply_to_raw",
        "from_emailaddress", "to_emailaddress", "cc_emailaddress", "bcc_emailaddress",
        "email_domains")) {
      if (doc.has(field)) {
        doc.removeField(field);
      }
    }
  }

  private static boolean isRecipientHeader(String cleanedFieldName) {
    return RECIPIENT_HEADER_FIELDS.contains(cleanedFieldName)
        || cleanedFieldName.startsWith("resent_")
        || "sender".equals(cleanedFieldName);
  }

  /**
   * Expands address arrays so each entry is a single recipient. If an entry is an unparsed multi-address header line
   * (comma-separated with multiple mailboxes), it is split via {@link InternetAddress#parseHeader}.
   */
  private static Address[] expandToIndividualAddresses(Address[] addresses) throws AddressException {
    if (addresses == null || addresses.length == 0) {
      return addresses;
    }
    List<Address> expanded = new ArrayList<>();
    for (Address address : addresses) {
      if (address == null) {
        continue;
      }
      if (address instanceof InternetAddress internetAddress
          && !appearsToBeMultipleAddresses(internetAddress.toString())) {
        expanded.add(internetAddress);
        continue;
      }
      InternetAddress[] parsed = InternetAddress.parseHeader(address.toString(), false);
      for (InternetAddress parsedAddress : parsed) {
        if (parsedAddress != null) {
          expanded.add(parsedAddress);
        }
      }
    }
    return expanded.toArray(new Address[0]);
  }

  private static boolean appearsToBeMultipleAddresses(String value) {
    if (value == null || !value.contains(",")) {
      return false;
    }
    try {
      return InternetAddress.parseHeader(value, false).length > 1;
    } catch (AddressException e) {
      return false;
    }
  }

  private static void addRecipientFields(
      Document doc, String fieldName, Address[] addresses, Set<String> emailDomains) {
    if (addresses == null) {
      return;
    }
    for (Address address : addresses) {
      if (address == null) {
        continue;
      }
      doc.addToField(fieldName + "_raw", address.toString());
      String email = extractEmailAddress(address);
      if (email != null && !email.isBlank()) {
        doc.addToField(fieldName, email);
        if (EMAIL_ADDRESS_FIELDS.contains(fieldName)) {
          doc.addToField(fieldName + "_emailaddress", email);
          String domain = extractDomain(email);
          if (domain != null && !domain.isBlank()) {
            emailDomains.add(domain);
          }
        }
      }
    }
  }

  private static void addRecipients(Document doc, String fieldName, Address[] addresses) {
    if (addresses == null) {
      return;
    }
    for (Address address : addresses) {
      if (address == null) {
        continue;
      }
      doc.addToField(fieldName + "_raw", address.toString());
      String email = extractEmailAddress(address);
      if (email != null && !email.isBlank()) {
        doc.addToField(fieldName, email);
      }
    }
  }

  /**
   * Returns the bare email address (user@domain) for an {@link Address}, or {@code null} when it cannot be determined.
   */
  static String extractEmailAddress(Address address) {
    if (address instanceof InternetAddress internetAddress) {
      String email = internetAddress.getAddress();
      return email != null ? email.trim().toLowerCase() : null;
    }
    try {
      InternetAddress[] parsed = InternetAddress.parse(address.toString(), false);
      if (parsed.length > 0 && parsed[0].getAddress() != null && !parsed[0].getAddress().isBlank()) {
        return parsed[0].getAddress().trim().toLowerCase();
      }
    } catch (AddressException e) {
      log.debug("Unable to parse email address from {}", address, e);
    }
    return null;
  }

  static String extractDomain(String email) {
    int at = email.lastIndexOf('@');
    if (at < 0 || at == email.length() - 1) {
      return null;
    }
    return email.substring(at + 1).trim().toLowerCase();
  }

  private static void parseContent(Part part, Document doc) throws Exception {
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

  public static boolean isExcludedHeader(String cleanedName, List<String> excludeHeaderPrefixes) {
    for (String prefix : excludeHeaderPrefixes) {
      if (cleanedName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  public static String cleanFieldName(String name) {
    return name.trim().toLowerCase().replaceAll("[ -]", "_");
  }

  public static List<String> normalizeExcludePrefixes(List<String> prefixes) {
    return prefixes.stream()
        .map(EmailMessageParser::cleanFieldName)
        .filter(prefix -> !prefix.isEmpty())
        .toList();
  }
}
