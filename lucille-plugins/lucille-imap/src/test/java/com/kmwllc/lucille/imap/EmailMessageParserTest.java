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
}
