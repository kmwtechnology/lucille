package com.kmwllc.lucille.imap.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.imap.EmailMessageParser;
import com.kmwllc.lucille.imap.connector.IMAPConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.junit.Test;

/**
 * Tests for {@link ParseMailMessage} and the end-to-end connector + stage email parsing flow.
 */
public class ParseMailMessageTest {

  private static final Session SESSION = Session.getInstance(new java.util.Properties());

  private IMAPConnector connector() {
    return new IMAPConnector(baseConfig());
  }

  private IMAPConnector connector(String docIdPrefix) {
    Config config = baseConfig().withFallback(
        ConfigFactory.parseString("docIdPrefix = \"" + docIdPrefix + "\""));
    return new IMAPConnector(config);
  }

  private Config baseConfig() {
    return ConfigFactory.parseString(
        "host = \"imap.example.com\"\n"
        + "username = \"user@example.com\"\n"
        + "password = \"secret\"\n");
  }

  private ParseMailMessage stage() {
    return new ParseMailMessage(ConfigFactory.parseString("name = \"parseMail\""));
  }

  private ParseMailMessage stage(Config config) {
    return new ParseMailMessage(config.withFallback(ConfigFactory.parseString("name = \"parseMail\"")));
  }

  private MimeMessage parse(String raw) throws Exception {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));
  }

  private Message messageWithReceivedDate(String raw, Date receivedDate) throws Exception {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8))) {
      @Override
      public Date getReceivedDate() {
        return receivedDate;
      }
    };
  }

  private Document parseEndToEnd(Message message) throws Exception {
    return parseEndToEnd(message, connector());
  }

  private Document parseEndToEnd(Message message, IMAPConnector connector) throws Exception {
    Document doc = connector.createDocumentFromMessage(message);
    stage().processDocument(doc);
    return doc;
  }

  @Test
  public void testSinglePlainTextMessage() throws Exception {
    Message message = parse("Message-ID: <abc@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Cc: carol@example.com\r\n"
        + "Subject: Hello World\r\n"
        + "Date: Wed, 1 Jan 2020 10:00:00 +0000\r\n"
        + "Content-Type: text/plain; charset=utf-8\r\n"
        + "\r\n"
        + "This is the body.");

    Document doc = parseEndToEnd(message);

    assertEquals("<abc@example.com>", doc.getId());
    assertEquals("Hello World", doc.getString("subject"));
    assertTrue(doc.getStringList("from").contains("alice@example.com"));
    assertTrue(doc.getStringList("to").contains("bob@example.com"));
    assertTrue(doc.getStringList("cc").contains("carol@example.com"));
    assertEquals(List.of("alice@example.com"), doc.getStringList("from_emailaddress"));
    assertEquals(List.of("bob@example.com"), doc.getStringList("to_emailaddress"));
    assertEquals(List.of("carol@example.com"), doc.getStringList("cc_emailaddress"));
    assertEquals(List.of("example.com"), doc.getStringList("email_domains"));
    assertTrue(doc.getStringList("text").contains("This is the body."));
    assertTrue(doc.has("sent_date"));
    assertTrue(doc.has("message_id"));
    assertFalse(doc.has(EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD));
  }

  @Test
  public void testReceivedDateFromImapIsPreservedAfterParsing() throws Exception {
    Date internalDate = Date.from(Instant.parse("2020-01-02T15:04:05Z"));
    Message message = messageWithReceivedDate("Message-ID: <received@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Received Date\r\n"
        + "Date: Wed, 1 Jan 2020 10:00:00 +0000\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body", internalDate);

    Document doc = parseEndToEnd(message);

    assertEquals(internalDate.toInstant(), doc.getInstant("received_date"));
    assertEquals(Instant.parse("2020-01-01T10:00:00Z"), doc.getInstant("sent_date"));
  }

  @Test
  public void testDocIdPrefixApplied() throws Exception {
    Message message = parse("Message-ID: <prefixed@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Prefixed\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = parseEndToEnd(message, connector("mail_"));

    assertEquals("mail_<prefixed@example.com>", doc.getId());
  }

  @Test
  public void testMultipartMessageExtractsTextAndHtml() throws Exception {
    MimeMessage message = new MimeMessage(SESSION);
    message.setFrom(new InternetAddress("alice@example.com"));
    message.setRecipients(Message.RecipientType.TO, "bob@example.com");
    message.setSubject("Multipart");

    MimeBodyPart textPart = new MimeBodyPart();
    textPart.setText("plain body", "utf-8");

    MimeBodyPart htmlPart = new MimeBodyPart();
    htmlPart.setContent("<p>html body</p>", "text/html; charset=utf-8");

    MimeMultipart multipart = new MimeMultipart("alternative");
    multipart.addBodyPart(textPart);
    multipart.addBodyPart(htmlPart);

    message.setContent(multipart);
    message.saveChanges();
    message.setHeader("Message-ID", "<multi@example.com>");

    Document doc = parseEndToEnd(message);

    assertEquals("<multi@example.com>", doc.getId());
    assertTrue(doc.getStringList("text").contains("plain body"));
    assertTrue(doc.getStringList("html").contains("<p>html body</p>"));
  }

  @Test
  public void testMessageWithoutMessageIdGetsGeneratedId() throws Exception {
    Message message = parse("From: alice@example.com\r\n"
        + "Subject: No Message Id\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = parseEndToEnd(message);

    assertNotNull(doc.getId());
    assertFalse(doc.getId().isBlank());
    assertEquals("No Message Id", doc.getString("subject"));
  }

  @Test
  public void testReservedHeaderNamesAreSkipped() throws Exception {
    Message message = parse("Message-ID: <reserved@example.com>\r\n"
        + "id: should-not-overwrite\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Reserved\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = parseEndToEnd(message);

    assertEquals("<reserved@example.com>", doc.getId());
  }

  @Test
  public void testNoisyXHeadersExcludedByDefault() throws Exception {
    Message message = parse("Message-ID: <noisy@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Noisy\r\n"
        + "X-Google-Smtp-Source: abcdef\r\n"
        + "X-Received: from somewhere\r\n"
        + "X-Mailer: SomeMailer 1.0\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = parseEndToEnd(message);

    assertEquals("Noisy", doc.getString("subject"));
    assertTrue(doc.has("message_id"));
    assertFalse(doc.has("x_google_smtp_source"));
    assertFalse(doc.has("x_received"));
    assertFalse(doc.has("x_mailer"));
  }

  @Test
  public void testEmptyExcludePrefixesKeepsAllHeaders() throws Exception {
    Config config = ConfigFactory.parseString("excludeHeaderPrefixes = []");
    Message message = parse("Message-ID: <keepall@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: KeepAll\r\n"
        + "X-Mailer: SomeMailer 1.0\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector().createDocumentFromMessage(message);
    stage(config).processDocument(doc);

    assertTrue(doc.has("x_mailer"));
  }

  @Test
  public void testCustomExcludePrefixes() throws Exception {
    Config config = ConfigFactory.parseString("excludeHeaderPrefixes = [\"dkim-\"]");
    Message message = parse("Message-ID: <custom@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Custom\r\n"
        + "DKIM-Signature: v=1; a=rsa-sha256\r\n"
        + "X-Mailer: SomeMailer 1.0\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector().createDocumentFromMessage(message);
    stage(config).processDocument(doc);

    assertFalse(doc.has("dkim_signature"));
    assertTrue(doc.has("x_mailer"));
  }

  @Test
  public void testDeleteRawMessageFieldCanBeDisabled() throws Exception {
    Config config = ConfigFactory.parseString("deleteRawMessageField = false");
    Message message = parse("Message-ID: <keepraw@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Keep Raw\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector().createDocumentFromMessage(message);
    stage(config).processDocument(doc);

    assertTrue(doc.has(EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD));
    assertEquals("Keep Raw", doc.getString("subject"));
  }
}
