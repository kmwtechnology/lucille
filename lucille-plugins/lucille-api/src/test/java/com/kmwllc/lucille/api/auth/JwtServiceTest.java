package com.kmwllc.lucille.api.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class JwtServiceTest {

  @Test
  public void testIssueThenVerifyReturnsSubject() {
    JwtService service = new JwtService();
    String token = service.issueToken("alice");

    Optional<String> subject = service.verifyToken(token);

    assertTrue(subject.isPresent());
    assertEquals("alice", subject.get());
  }

  @Test
  public void testVerifyRejectsGarbage() {
    JwtService service = new JwtService();
    assertTrue(service.verifyToken("not-a-token").isEmpty());
    assertTrue(service.verifyToken("").isEmpty());
  }

  @Test
  public void testVerifyRejectsTokenFromDifferentKey() {
    // Each JwtService generates its own key, so a token from one must not verify under another.
    JwtService issuer = new JwtService();
    JwtService other = new JwtService();
    String token = issuer.issueToken("alice");

    assertTrue(other.verifyToken(token).isEmpty());
  }

  @Test
  public void testVerifyRejectsExpiredToken() throws Exception {
    JwtService service = new JwtService(Duration.ofMillis(1));
    String token = service.issueToken("alice");
    Thread.sleep(50);

    assertTrue(service.verifyToken(token).isEmpty());
  }

  @Test
  public void testVerifyRejectsTamperedToken() {
    JwtService service = new JwtService();
    String token = service.issueToken("alice");
    // Change the FIRST character of the signature; still well-formed, but the signature no longer matches.
    // (Not the last: a 32-byte signature is 43 base64url chars and the final char carries only 4 significant
    // bits, so altering it can leave the decoded bytes unchanged and the token would still verify.)
    int sigStart = token.lastIndexOf('.') + 1;
    char first = token.charAt(sigStart);
    String tampered = token.substring(0, sigStart) + (first == 'A' ? 'B' : 'A') + token.substring(sigStart + 1);

    assertFalse(service.verifyToken(tampered).isPresent());
  }
}
