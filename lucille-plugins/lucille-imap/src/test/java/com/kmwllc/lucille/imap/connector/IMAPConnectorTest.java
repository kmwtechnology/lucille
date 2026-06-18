package com.kmwllc.lucille.imap.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
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
import org.junit.Test;

/**
 * Tests for {@link IMAPConnector}. These exercise the message-to-Document conversion directly against real
 * {@link MimeMessage} instances, avoiding the need for a live IMAP server or mocked mail objects.
 */
public class IMAPConnectorTest {

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

  private MimeMessage parse(String raw) throws Exception {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));
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

    Document doc = connector().processMessage(message);

    assertEquals("<abc@example.com>", doc.getId());
    assertEquals("Hello World", doc.getString("subject"));
    assertTrue(doc.getStringList("from").contains("alice@example.com"));
    assertTrue(doc.getStringList("to").contains("bob@example.com"));
    assertTrue(doc.getStringList("cc").contains("carol@example.com"));
    assertTrue(doc.getStringList("text").contains("This is the body."));
    assertTrue(doc.has("sent_date"));
    // The raw Message-ID header should be copied as a field too.
    assertTrue(doc.has("message_id"));
  }

  @Test
  public void testDocIdPrefixApplied() throws Exception {
    Message message = parse("Message-ID: <prefixed@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Prefixed\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector("mail_").processMessage(message);

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
    // saveChanges() regenerates the Message-ID, so set our deterministic id afterwards.
    message.saveChanges();
    message.setHeader("Message-ID", "<multi@example.com>");

    Document doc = connector().processMessage(message);

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

    Document doc = connector().processMessage(message);

    assertNotNull(doc.getId());
    assertFalse(doc.getId().isBlank());
    assertEquals("No Message Id", doc.getString("subject"));
  }

  @Test
  public void testReservedHeaderNamesAreSkipped() throws Exception {
    // A header literally named "id" must not clobber the reserved Document id field.
    Message message = parse("Message-ID: <reserved@example.com>\r\n"
        + "id: should-not-overwrite\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Reserved\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector().processMessage(message);

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

    Document doc = connector().processMessage(message);

    // Standard headers are still copied...
    assertEquals("Noisy", doc.getString("subject"));
    assertTrue(doc.has("message_id"));
    // ...but the X-* family is excluded by default.
    assertFalse(doc.has("x_google_smtp_source"));
    assertFalse(doc.has("x_received"));
    assertFalse(doc.has("x_mailer"));
  }

  @Test
  public void testEmptyExcludePrefixesKeepsAllHeaders() throws Exception {
    Config config = baseConfig().withFallback(
        ConfigFactory.parseString("excludeHeaderPrefixes = []"));
    IMAPConnector connector = new IMAPConnector(config);

    Message message = parse("Message-ID: <keepall@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: KeepAll\r\n"
        + "X-Mailer: SomeMailer 1.0\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector.processMessage(message);

    // With no exclusions configured, the X-* headers are retained.
    assertTrue(doc.has("x_mailer"));
  }

  @Test
  public void testCustomExcludePrefixes() throws Exception {
    // Prefixes are matched against the cleaned (lower-cased, underscore) header name; "dkim-" -> "dkim_".
    Config config = baseConfig().withFallback(
        ConfigFactory.parseString("excludeHeaderPrefixes = [\"dkim-\"]"));
    IMAPConnector connector = new IMAPConnector(config);

    Message message = parse("Message-ID: <custom@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Custom\r\n"
        + "DKIM-Signature: v=1; a=rsa-sha256\r\n"
        + "X-Mailer: SomeMailer 1.0\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector.processMessage(message);

    // Only the configured prefix is excluded; the default x_ exclusion no longer applies.
    assertFalse(doc.has("dkim_signature"));
    assertTrue(doc.has("x_mailer"));
  }
}
