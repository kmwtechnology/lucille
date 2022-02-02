package com.kmwllc.lucille;

import java.time.LocalDate;
import java.util.function.Function;

public class CustomParseDate implements Function<String, LocalDate> {

  public CustomParseDate() {

  }

  @Override
  public LocalDate apply(String s) {
    System.out.println(LocalDate.parse(cleanDate(s)));
    return LocalDate.parse(cleanDate(s));

  }

  public static String cleanDate(String inputDate) throws IllegalArgumentException {
    if (inputDate == null) {
      throw new IllegalArgumentException("input date cannot be null");
    } else if (inputDate.length() <= 4) {
      return inputDate + "-01-01";
    } else if (inputDate.length() <= 7) {
      return inputDate + "-01";
    } else {
      return inputDate;
    }
  }
}
