package com.predisched.common.auth;

import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import javax.crypto.SecretKey;

/**
 * HS256 JSON Web Tokens (jjwt). The subject is the client id; the token must be signed with the
 * shared secret and not expired. The secret comes from an environment variable and must be at
 * least 32 bytes, as HS256 requires.
 */
public final class JwtVerifier {

    /** Why a token was refused. */
    public static final class InvalidTokenException extends Exception {
        public InvalidTokenException(String message) {
            super(message);
        }
    }

    private final SecretKey key;

    public JwtVerifier(String secret) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    /** The client id in a valid token. */
    public String verify(String token) throws InvalidTokenException {
        try {
            String subject = Jwts.parser().verifyWith(key).build()
                    .parseSignedClaims(token).getPayload().getSubject();
            if (subject == null || subject.isBlank()) {
                throw new InvalidTokenException("token has no subject");
            }
            return subject;
        } catch (ExpiredJwtException e) {
            throw new InvalidTokenException("token expired");
        } catch (JwtException | IllegalArgumentException e) {
            throw new InvalidTokenException("invalid token: " + e.getMessage());
        }
    }

    /** Issues a token for a client, for the CLI and tests. */
    public String issue(String clientId, Instant expiresAt) {
        return Jwts.builder()
                .subject(clientId)
                .issuedAt(new Date())
                .expiration(Date.from(expiresAt))
                .signWith(key)
                .compact();
    }
}
