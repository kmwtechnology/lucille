package com.kmwllc.lucille.imap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.Test;

public class EmailMessageParserTest {

  private static final Session SESSION = Session.getInstance(new java.util.Properties());

  private MimeMessage parse(String raw) throws Exception {
    return new MimeMessage(SESSION, new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testMultipleRecipientsAreMultiValued() throws Exception {
    Message message = parse("Message-ID: <multi@example.com>\r\n"
        + "From: Alice <alice@example.com>\r\n"
        + "To: Bob <bob@example.com>, Carol <carol@other.org>\r\n"
        + "Cc: Dave <dave@example.com>\r\n"
        + "Bcc: Eve <eve@other.org>\r\n"
        + "Subject: Multi\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<multi@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of("alice@example.com"), doc.getStringList("from"));
    assertEquals(List.of("bob@example.com", "carol@other.org"), doc.getStringList("to"));
    assertEquals(List.of("dave@example.com"), doc.getStringList("cc"));
    assertEquals(List.of("eve@other.org"), doc.getStringList("bcc"));

    assertTrue(doc.getStringList("from_raw").get(0).contains("alice@example.com"));
    assertEquals(2, doc.getStringList("to_raw").size());

    assertEquals(List.of("alice@example.com"), doc.getStringList("from_emailaddress"));
    assertEquals(List.of("bob@example.com", "carol@other.org"), doc.getStringList("to_emailaddress"));
    assertEquals(List.of("dave@example.com"), doc.getStringList("cc_emailaddress"));
    assertEquals(List.of("eve@other.org"), doc.getStringList("bcc_emailaddress"));

    assertEquals(List.of("example.com", "other.org"), doc.getStringList("email_domains"));
  }

  @Test
  public void testEmailDomainsAreUnique() throws Exception {
    Message message = parse("Message-ID: <domains@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com, carol@example.com\r\n"
        + "Subject: Domains\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<domains@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of("example.com"), doc.getStringList("email_domains"));
  }

  @Test
  public void testRecipientHeadersAreNotCopiedFromRawHeaders() throws Exception {
    Message message = parse("Message-ID: <headers@example.com>\r\n"
        + "From: Alice <alice@example.com>\r\n"
        + "To: bob@example.com, carol@other.org\r\n"
        + "Subject: Headers\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<headers@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    // Raw To header would be a single comma-separated string; structured parsing yields two values.
    assertEquals(2, doc.getStringList("to").size());
    assertEquals(2, doc.getStringList("to_raw").size());
    assertTrue(doc.getStringList("to").contains("bob@example.com"));
    assertTrue(doc.getStringList("to").contains("carol@other.org"));
    assertTrue(doc.getStringList("to_emailaddress").contains("bob@example.com"));
    assertTrue(doc.getStringList("to_emailaddress").contains("carol@other.org"));
  }

  @Test
  public void testFoldedMultiRecipientToHeader() throws Exception {
    Message message = parse("Message-ID: <folded@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: Kevin Watters <kwatters@kmwllc.com>, Rudi Seitz <rudi@kmwllc.com>, Brian\r\n"
        + " Nauheimer <brian@kmwllc.com>, Spencer Solomon <spencer.solomon@kmwllc.com>\r\n"
        + "Subject: Folded\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<folded@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of(
        "kwatters@kmwllc.com",
        "rudi@kmwllc.com",
        "brian@kmwllc.com",
        "spencer.solomon@kmwllc.com"), doc.getStringList("to"));
    assertEquals(4, doc.getStringList("to_raw").size());
    for (String raw : doc.getStringList("to_raw")) {
      assertFalse("to_raw must not contain a combined multi-recipient header line", raw.contains(","));
    }
  }

  @Test
  public void testPreexistingToFieldIsReplaced() throws Exception {
    Message message = parse("Message-ID: <replace@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Subject: Replace\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<replace@example.com>");
    doc.addToField("to", "Kevin Watters <kwatters@kmwllc.com>, Rudi Seitz <rudi@kmwllc.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of("bob@example.com"), doc.getStringList("to"));
  }

  @Test
  public void testExtractEmailAddressFromInternetAddress() throws Exception {
    InternetAddress address = new InternetAddress("alice@example.com", "Alice");
    assertEquals("alice@example.com", EmailMessageParser.extractEmailAddress(address));
  }

  @Test
  public void testMislabeledBase64TextPartIsStillParsed() throws Exception {
    Message message = parse("Message-ID: <mislabeled@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Subject: Mislabeled\r\n"
        + "MIME-Version: 1.0\r\n"
        + "Content-Type: multipart/alternative; boundary=\"b\"\r\n"
        + "\r\n"
        + "--b\r\n"
        + "Content-Type: text/plain; charset=UTF-8\r\n"
        + "Content-Transfer-Encoding: base64\r\n"
        + "\r\n"
        + "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html\"></head>"
        + "<body>plain text body</body></html>\r\n"
        + "--b\r\n"
        + "Content-Type: text/html; charset=UTF-8\r\n"
        + "Content-Transfer-Encoding: 7bit\r\n"
        + "\r\n"
        + "<html><body>html body</body></html>\r\n"
        + "--b--\r\n");

    Document doc = Document.create("<mislabeled@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertTrue(doc.has("text"));
    assertTrue(doc.getStringList("text").get(0).contains("plain text body"));
    assertTrue(doc.has("html"));
    assertTrue(doc.getStringList("html").get(0).contains("html body"));
  }

  @Test
  public void testBase64TextPartWithTrailingMetadataIsParsed() throws Exception {
    String body = java.util.Base64.getEncoder().encodeToString("Hello world.".getBytes(StandardCharsets.UTF_8));
    Message message = parse("Message-ID: <trailing@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Subject: Trailing metadata\r\n"
        + "Content-Type: text/plain; charset=UTF-8\r\n"
        + "Content-Transfer-Encoding: base64\r\n"
        + "\r\n"
        + body + "\r\n"
        + "<XHTML-STRIPONREPLY></XHTML-STRIPONREPLY>\r\n");

    Document doc = Document.create("<trailing@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of("Hello world."), doc.getStringList("text"));
  }

  @Test
  public void testMalformedContentTypeWithUnquotedNameParameterIsParsed() throws Exception {
    Message message = parse("Message-ID: <attached@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Subject: Attached\r\n"
        + "MIME-Version: 1.0\r\n"
        + "Content-Type: multipart/mixed; boundary=\"b\"\r\n"
        + "\r\n"
        + "--b\r\n"
        + "Content-Type: text/plain; charset=US-ASCII; name=Attached Message Part\r\n"
        + "Content-Transfer-Encoding: 7bit\r\n"
        + "\r\n"
        + "attached message body\r\n"
        + "--b--\r\n");

    Document doc = Document.create("<attached@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(List.of("attached message body"), doc.getStringList("text"));
  }

  @Test
  public void testReceivedDateFromReceivedHeaderWhenImapDateAbsent() throws Exception {
    Message message = parse("Message-ID: <received-header@example.com>\r\n"
        + "From: alice@example.com\r\n"
        + "To: bob@example.com\r\n"
        + "Subject: Received Header\r\n"
        + "Received: from mail.example.com by mx.google.com; Wed, 2 Jan 2020 15:04:05 +0000\r\n"
        + "Content-Type: text/plain\r\n"
        + "\r\n"
        + "body");

    Document doc = Document.create("<received-header@example.com>");
    EmailMessageParser.populateDocument(message, doc);

    assertEquals(Instant.parse("2020-01-02T15:04:05Z"), doc.getInstant("received_date"));
  }
}
