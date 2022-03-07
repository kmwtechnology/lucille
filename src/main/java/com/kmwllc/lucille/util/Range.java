package com.kmwllc.lucille.util;

/**
 * Simple POJO to represent a range of integers.  Range objects have a start
 * and end and can be compared to other Ranges.  Range objects are immutable.
 * <p/>
 * Created by matt on 9/18/17.
 */
public class Range implements Comparable<Range> {
  private final int start;
  private final int end;

  /**
   * Constructs a new Range with specified start and end
   * @param start Start of range
   * @param end End of range
   */
  public Range(int start, int end) {
    this.start = start;
    this.end = end;
  }

  /**
   * Get the start of this Range
   * @return Start position
   */
  public int getStart() {
    return start;
  }

  /**
   * Get the end of this Range
   * @return End position
   */
  public int getEnd() {
    return end;
  }

  /**
   * Get the size of this Range.  (i.e. end - start)
   * @return Size of range
   */
  public int size() {
    return end - start;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Range range = (Range) o;

    if (start != range.start) return false;
    return end == range.end;
  }

  @Override
  public int hashCode() {
    int result = start;
    result = 31 * result + end;
    return result;
  }

  @Override
  public int compareTo(Range o) {
    if (start == o.getStart()) {
      return end - o.getEnd();
    }
    return start - o.getStart();
  }
}
