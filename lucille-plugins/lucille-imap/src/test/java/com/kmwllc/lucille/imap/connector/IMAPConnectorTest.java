package com.kmwllc.lucille.imap.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.imap.EmailMessageParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import org.junit.Test;

/**
 * Tests for {@link IMAPConnector} connector configuration and lightweight document creation. Full email parsing is
 * covered by {@link com.kmwllc.lucille.imap.stage.ParseMailMessageTest}.
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

  private Message messageWithReceivedDate(String raw, Date receivedDate) throws Exception {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8))) {
      @Override
      public Date getReceivedDate() {
        return receivedDate;
      }
    };
  }

  @Test
  public void testCreateDocumentSetsIdAndRawBytesOnly() throws Exception {
    Message message = parse("Message-ID: <abc@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Hello World\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "This is the body.");

    Document doc = connector().createDocumentFromMessage(message);

    assertEquals("<abc@example.com>", doc.getId());
    assertTrue(doc.has(EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD));
    assertNotNull(doc.getBytes(EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD));
    assertTrue(doc.getBytes(EmailMessageParser.DEFAULT_RAW_MESSAGE_FIELD).length > 0);
    // Parsed fields are populated downstream by ParseMailMessage.
    assertFalse(doc.has("subject"));
    assertFalse(doc.has("text"));
  }

  @Test
  public void testCreateDocumentSetsReceivedDateFromImapInternalDate() throws Exception {
    Date internalDate = Date.from(Instant.parse("2020-01-02T15:04:05Z"));
    Message message = messageWithReceivedDate("Message-ID: <received@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Received Date\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body", internalDate);

    Document doc = connector().createDocumentFromMessage(message);

    assertEquals(internalDate.toInstant(), doc.getInstant("received_date"));
  }

  @Test
  public void testDocIdPrefixApplied() throws Exception {
    Message message = parse("Message-ID: <prefixed@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "Subject: Prefixed\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector("mail_").createDocumentFromMessage(message);

    assertEquals("mail_<prefixed@example.com>", doc.getId());
  }

  @Test
  public void testMessageWithoutMessageIdGetsGeneratedId() throws Exception {
    Message message = parse("From: alice@example.com\r\n"
        + "Subject: No Message Id\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = connector().createDocumentFromMessage(message);

    assertNotNull(doc.getId());
    assertFalse(doc.getId().isBlank());
  }

  @Test
  public void testFoldersDefaultsToInbox() {
    assertEquals(java.util.List.of("INBOX"), connector().getFolderNames());
  }

  @Test
  public void testFoldersListIsParsed() {
    Config config = baseConfig().withFallback(
        ConfigFactory.parseString("folders = [\"INBOX\", \"[Gmail]/Sent Mail\"]"));
    IMAPConnector connector = new IMAPConnector(config);

    assertEquals(java.util.List.of("INBOX", "[Gmail]/Sent Mail"), connector.getFolderNames());
  }

  @Test
  public void testEmptyFoldersListFallsBackToInbox() {
    Config config = baseConfig().withFallback(ConfigFactory.parseString("folders = []"));
    IMAPConnector connector = new IMAPConnector(config);

    assertEquals(java.util.List.of("INBOX"), connector.getFolderNames());
  }
}
