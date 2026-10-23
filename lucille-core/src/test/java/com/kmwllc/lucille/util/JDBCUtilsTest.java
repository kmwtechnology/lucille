package com.kmwllc.lucille.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.Test;

/**
 * Seam tests for {@link JDBCUtils}'s temporal handling, exercised directly against a mocked
 * {@link ResultSet} so they are deterministic and independent of any database, driver, or the JVM's
 * default time zone.
 *
 * <p>These tests document a silent corruption: a zone-less SQL DATE/TIMESTAMP is read with the no-arg
 * {@code getTimestamp()}/{@code getDate()}, which the driver resolves against a time zone and collapses
 * to an {@code Instant}; the stored value is then an ISO-8601 (zulu) string of that instant. When the
 * resolving zone is not UTC, the stored wall-clock value is shifted by that zone's offset (and a DATE can
 * roll to a different calendar day). The mocked ResultSet here returns the exact
 * {@code java.sql.Timestamp}/{@code java.sql.Date} a driver in a UTC-5 zone would produce for the literals
 * {@code TIMESTAMP '2023-01-01 00:00:00'} and {@code DATE '2023-06-15'}.
 *
 * <p>The assertions are resolution-agnostic: they only require that the stored string preserves the
 * original wall-clock value, which holds for any faithful representation a fix might choose. They fail
 * today because the value is shifted.
 */
public class JDBCUtilsTest {

  /** Fixed offsets (no daylight saving), used to build inputs with an explicit, known offset. */
  private static final ZoneOffset UTC_MINUS_5 = ZoneOffset.ofHours(-5);
  private static final ZoneOffset UTC_PLUS_5 = ZoneOffset.ofHours(5);

  @Test
  public void testTimestampKeepsWallClockValueRegardlessOfResolvingZone() throws Exception {
    // The wall-clock value stored in the database, with no zone attached.
    LocalDateTime wallClock = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
    // The java.sql.Timestamp a driver in a UTC-5 zone hands back for that literal: the same wall-clock,
    // whose underlying instant is 2023-01-01T05:00:00Z.
    Timestamp driverValue = Timestamp.from(wallClock.toInstant(UTC_MINUS_5));

    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnType(1)).thenReturn(Types.TIMESTAMP);
    when(rs.getTimestamp(1)).thenReturn(driverValue);

    Document doc = Document.create("id");
    JDBCUtils.parseResultToDoc(doc, rs, "ts", 1);

    String stored = doc.getString("ts");
    assertNotNull("TIMESTAMP column should be stored on the document", stored);
    // The original wall-clock time (00:00:00) must be preserved, not shifted to 05:00:00 by the
    // resolving zone's offset.
    assertTrue("TIMESTAMP must keep its wall-clock value, not be shifted by the resolving zone "
            + "(was: " + stored + ")",
        stored.startsWith("2023-01-01T00:00:00"));
  }

  /**
   * Vendor-specific column type codes (Oracle TIMESTAMP WITH TIME ZONE is -101, SQL Server DATETIMEOFFSET
   * is -155) are not members of {@link Types}, so {@link java.sql.JDBCType#valueOf(int)} throws for them.
   * H2 cannot produce such a code, so this is checked against a mocked ResultSet.
   *
   * <p>Under the strict contract an unsupported type must fail, so an exception is expected; but it must be
   * a deliberate, diagnosable failure that names the field (and ideally the code), not the accidental
   * {@code IllegalArgumentException: Type:-101 is not a valid Types.java value} raised while trying to
   * build a log message. The test is agnostic about the exception class.
   */
  @Test
  public void testVendorSpecificTypeCodeFailsWithDiagnosableMessage() throws Exception {
    int oracleTimestampWithTimeZone = -101;

    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnType(1)).thenReturn(oracleTimestampWithTimeZone);
    when(meta.getColumnTypeName(1)).thenReturn("TIMESTAMP WITH TIME ZONE");

    Document doc = Document.create("id");
    Exception thrown = assertThrows(Exception.class, () -> JDBCUtils.parseResultToDoc(doc, rs, "weird_col", 1));

    String message = String.valueOf(thrown.getMessage());
    assertFalse("the failure must be deliberate, not JDBCType.valueOf failing inside the warning (was: "
            + message + ")",
        message.contains("is not a valid Types.java value"));
    assertTrue("the failure must name the offending field (was: " + message + ")",
        message.contains("weird_col"));
    assertFalse("nothing must be stored for an unsupported column", doc.has("weird_col"));
  }

  @Test
  public void testDateKeepsCalendarDayRegardlessOfResolvingZone() throws Exception {
    LocalDate day = LocalDate.of(2023, 6, 15);
    // The java.sql.Date a driver in a UTC+5 zone hands back for DATE '2023-06-15': midnight local,
    // whose underlying instant is 2023-06-14T19:00:00Z. Collapsing to a UTC instant therefore rolls the
    // calendar day back to the 14th - the most visible form of this corruption.
    Date driverValue = new Date(day.atStartOfDay().toInstant(UTC_PLUS_5).toEpochMilli());

    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnType(1)).thenReturn(Types.DATE);
    when(rs.getDate(1)).thenReturn(driverValue);

    Document doc = Document.create("id");
    JDBCUtils.parseResultToDoc(doc, rs, "d", 1);

    String stored = doc.getString("d");
    assertNotNull("DATE column should be stored on the document", stored);
    // The calendar day (2023-06-15) must be preserved, not shifted by the resolving zone's offset.
    assertTrue("DATE must keep its calendar day, not be shifted by the resolving zone "
            + "(was: " + stored + ")",
        stored.startsWith("2023-06-15"));
  }
}
