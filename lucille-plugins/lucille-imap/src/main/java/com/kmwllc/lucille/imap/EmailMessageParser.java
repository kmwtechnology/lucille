package com.kmwllc.lucille.imap;

import com.kmwllc.lucille.core.Document;
import jakarta.mail.Address;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.internet.ContentType;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MailDateFormat;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.internet.MimeUtility;
import jakarta.mail.internet.AddressException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private static final Session SESSION = createMailSession();

  // Recipient headers are populated from structured Address objects (multi-valued, one entry per address) rather
  // than copied from the raw header lines, which are often a single comma-separated string.
  private static final Set<String> RECIPIENT_HEADER_FIELDS = Set.of("from", "to", "cc", "bcc", "reply_to");
  private static final Set<String> EMAIL_ADDRESS_FIELDS = Set.of("from", "to", "cc", "bcc");
  private static final Pattern CHARSET_PARAMETER =
      Pattern.compile("(?i)charset\\s*=\\s*([^;\\s]+|\"[^\"]+\")");

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
    setReceivedDate(doc, message);

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

  /**
   * Populates {@code received_date} from IMAP INTERNALDATE (often pre-set on the document by {@link
   * com.kmwllc.lucille.imap.connector.IMAPConnector}), the parsed message's received date, or mail headers as a
   * fallback.
   */
  private static void setReceivedDate(Document doc, Message message) throws MessagingException {
    if (doc.has("received_date")) {
      return;
    }

    Date receivedDate = message.getReceivedDate();
    if (receivedDate != null) {
      doc.setField("received_date", receivedDate.toInstant());
      return;
    }

    setReceivedDateFromHeaders(doc, message);
  }

  private static void setReceivedDateFromHeaders(Document doc, Message message) throws MessagingException {
    String[] received = message.getHeader("Received");
    if (received != null && received.length > 0) {
      Date parsed = parseReceivedHeaderDate(received[0]);
      if (parsed != null) {
        doc.setField("received_date", parsed.toInstant());
        return;
      }
    }

    String[] deliveryDate = message.getHeader("Delivery-Date");
    if (deliveryDate != null && deliveryDate.length > 0 && deliveryDate[0] != null && !deliveryDate[0].isBlank()) {
      try {
        Date parsed = new MailDateFormat().parse(deliveryDate[0].trim());
        if (parsed != null) {
          doc.setField("received_date", parsed.toInstant());
        }
      } catch (ParseException e) {
        log.debug("Unable to parse Delivery-Date header into a received_date.", e);
      }
    }
  }

  private static Date parseReceivedHeaderDate(String received) {
    int semi = received.lastIndexOf(';');
    if (semi < 0 || semi == received.length() - 1) {
      return null;
    }
    try {
      return new MailDateFormat().parse(received.substring(semi + 1).trim());
    } catch (ParseException e) {
      log.debug("Unable to parse Received header date from '{}'.", received, e);
      return null;
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

  private static Session createMailSession() {
    Properties props = new Properties();
    // Gmail and other providers sometimes encode message/rfc822 attachments contrary to RFC 2046.
    props.setProperty("mail.mime.allowencodedmessages", "true");
    return Session.getInstance(props);
  }

  private static void parseContent(Part part, Document doc) throws Exception {
    if (part.isMimeType("multipart/*")) {
      MimeMultipart multipart = (MimeMultipart) part.getContent();
      for (int i = 0; i < multipart.getCount(); i++) {
        parseContent(multipart.getBodyPart(i), doc);
      }
      return;
    }

    if (part.isMimeType("message/rfc822")) {
      Message nested = readNestedMessage(part);
      if (nested != null) {
        parseContent(nested, doc);
      }
      return;
    }

    if (part.isMimeType("text/plain")) {
      String text = readTextPart(part);
      if (text != null) {
        doc.addToField("text", text);
      }
      return;
    }

    if (part.isMimeType("text/html")) {
      String html = readTextPart(part);
      if (html != null) {
        doc.addToField("html", html);
      }
      return;
    }

    log.debug("Skipping unhandled content type {}", part.getContentType());
  }

  private static Message readNestedMessage(Part part) {
    try {
      Object content = part.getContent();
      if (content instanceof Message message) {
        return message;
      }
    } catch (Exception e) {
      if (isTransferDecodingFailure(e)) {
        try (InputStream raw = rawInputStream(part)) {
          return new MimeMessage(SESSION, raw);
        } catch (Exception fallback) {
          log.warn("Unable to read nested message/rfc822 part, skipping it.", e);
          return null;
        }
      }
      log.warn("Unable to read nested message/rfc822 part, skipping it.", e);
    }
    return null;
  }

  private static String readTextPart(Part part) {
    try {
      Object content = part.getContent();
      if (content instanceof String string) {
        return string;
      }
      if (content instanceof InputStream inputStream) {
        try (inputStream) {
          return new String(inputStream.readAllBytes(), charsetFor(part));
        }
      }
      return String.valueOf(content);
    } catch (Exception e) {
      if (!isTransferDecodingFailure(e)) {
        log.warn("Unable to read content for a message part, skipping it.", e);
        return null;
      }
      try {
        return readTextFromRawPart(part);
      } catch (Exception fallback) {
        log.warn("Unable to read content for a message part, skipping it.", e);
        return null;
      }
    }
  }

  private static String readTextFromRawPart(Part part) throws MessagingException, IOException {
    byte[] raw;
    try (InputStream rawStream = rawInputStream(part)) {
      raw = rawStream.readAllBytes();
    }

    String transferEncoding = transferEncoding(part);
    if ("base64".equalsIgnoreCase(transferEncoding)) {
      byte[] decoded = decodeBase64Lenient(raw);
      if (decoded != null) {
        return new String(decoded, charsetFor(part));
      }
      if (looksLikeText(raw)) {
        log.debug(
            "Part declares base64 Content-Transfer-Encoding but raw content appears to be plain text; using raw bytes.");
        return new String(raw, charsetFor(part));
      }
    } else if ("quoted-printable".equalsIgnoreCase(transferEncoding)) {
      try (InputStream decoded = MimeUtility.decode(new ByteArrayInputStream(raw), "quoted-printable")) {
        return new String(decoded.readAllBytes(), charsetFor(part));
      }
    }

    return new String(raw, charsetFor(part));
  }

  private static InputStream rawInputStream(Part part) throws MessagingException {
    if (part instanceof MimeBodyPart mimeBodyPart) {
      return mimeBodyPart.getRawInputStream();
    }
    if (part instanceof MimeMessage mimeMessage) {
      return mimeMessage.getRawInputStream();
    }
    throw new MessagingException("Cannot read raw content stream for " + part.getClass().getName());
  }

  private static String transferEncoding(Part part) throws MessagingException {
    String[] values = part.getHeader("Content-Transfer-Encoding");
    if (values == null || values.length == 0 || values[0] == null) {
      return null;
    }
    return values[0].trim();
  }

  private static byte[] decodeBase64Lenient(byte[] raw) {
    StringBuilder encoded = new StringBuilder();
    for (String line : new String(raw, StandardCharsets.US_ASCII).split("\\r?\\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if (!trimmed.matches("^[A-Za-z0-9+/=]+$")) {
        break;
      }
      encoded.append(trimmed);
    }
    if (encoded.isEmpty()) {
      return null;
    }
    try {
      return Base64.getMimeDecoder().decode(encoded.toString());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static boolean looksLikeText(byte[] raw) {
    int sampleLength = Math.min(raw.length, 512);
    if (sampleLength == 0) {
      return false;
    }
    int printable = 0;
    for (int i = 0; i < sampleLength; i++) {
      byte value = raw[i];
      if (value == '\t' || value == '\n' || value == '\r' || (value >= 32 && value < 127)) {
        printable++;
      }
    }
    return printable > sampleLength * 0.85;
  }

  private static boolean isTransferDecodingFailure(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current.getClass().getName().contains("DecodingException")) {
        return true;
      }
    }
    String message = error.getMessage();
    return message != null && message.contains("BASE64Decoder");
  }

  private static Charset charsetFor(Part part) {
    String contentType;
    try {
      contentType = part.getContentType();
    } catch (MessagingException e) {
      log.debug("Unable to read Content-Type; defaulting to UTF-8.", e);
      return StandardCharsets.UTF_8;
    }
    if (contentType == null) {
      return StandardCharsets.UTF_8;
    }

    String charset = null;
    try {
      charset = new ContentType(contentType).getParameter("charset");
    } catch (jakarta.mail.internet.ParseException e) {
      log.debug("Malformed Content-Type header '{}'; extracting charset leniently.", contentType);
      charset = extractCharsetLenient(contentType);
    }
    if (charset == null || charset.isBlank()) {
      return StandardCharsets.UTF_8;
    }
    try {
      return Charset.forName(charset.trim());
    } catch (IllegalArgumentException e) {
      log.debug("Unknown charset '{}' in Content-Type '{}'; defaulting to UTF-8.", charset, contentType);
      return StandardCharsets.UTF_8;
    }
  }

  private static String extractCharsetLenient(String contentType) {
    Matcher matcher = CHARSET_PARAMETER.matcher(contentType);
    if (!matcher.find()) {
      return null;
    }
    String charset = matcher.group(1).trim();
    if (charset.startsWith("\"") && charset.endsWith("\"") && charset.length() >= 2) {
      charset = charset.substring(1, charset.length() - 1);
    }
    return charset;
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
