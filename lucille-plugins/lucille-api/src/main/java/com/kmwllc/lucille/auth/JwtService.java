package com.kmwllc.lucille.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Issues and verifies the short-lived HS256 bearer tokens used by the Admin UI login flow. The
 * signing key is generated fresh per instance, so tokens do not survive an API restart.
 */
public class JwtService {

  private static final Logger log = LoggerFactory.getLogger(JwtService.class);

  /** How long an issued token remains valid. */
  public static final Duration TOKEN_TTL = Duration.ofHours(4);

  private final SecretKey key;
  private final Duration ttl;

  /**
   * Constructs a JwtService with a freshly generated signing key and the default 4-hour token TTL.
   */
  public JwtService() {
    this(TOKEN_TTL);
  }

  /**
   * Constructs a JwtService with a freshly generated signing key and the given token TTL.
   * @param ttl how long issued tokens remain valid
   */
  public JwtService(Duration ttl) {
    this.key = Jwts.SIG.HS256.key().build();
    this.ttl = ttl;
    log.info("JWT signing key generated; issued tokens expire after {}.", ttl);
  }

  /**
   * Issues a signed token for the given subject, valid for the configured TTL.
   * @param subject the token subject
   * @return a signed, compact JWT string
   */
  public String issueToken(String subject) {
    Instant now = Instant.now();
    return Jwts.builder()
        .subject(subject)
        .issuedAt(Date.from(now))
        .expiration(Date.from(now.plus(ttl)))
        .signWith(key)
        .compact();
  }

  /**
   * Verifies a token's signature and expiry and returns its subject.
   * @param token the compact JWT string
   * @return the token subject when valid; empty when the token is malformed, mis-signed, or expired
   */
  public Optional<String> verifyToken(String token) {
    try {
      Claims claims = Jwts.parser()
          .verifyWith(key)
          .build()
          .parseSignedClaims(token)
          .getPayload();
      return Optional.ofNullable(claims.getSubject());
    } catch (JwtException | IllegalArgumentException e) {
      // bad signature, expired, malformed, or null/empty input
      return Optional.empty();
    }
  }
}
