package com.kmwllc.lucille.util;

/**
 * Created by matt on 9/18/17.
 */
public class Range implements Comparable<Range> {
  int start;
  int end;

  public Range(int start, int end) {
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }

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