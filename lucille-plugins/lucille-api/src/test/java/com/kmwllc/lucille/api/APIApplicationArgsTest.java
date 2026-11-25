package com.kmwllc.lucille.api;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class APIApplicationArgsTest {

  @Test
  public void testNoArgsDefaultsToApiYml() {
    assertArrayEquals(new String[] {"server", "conf/api.yml"}, APIApplication.withDefaultArgs(new String[0]));
  }

  @Test
  public void testGivenArgsPassThrough() {
    String[] args = {"server", "my.yml"};
    assertArrayEquals(args, APIApplication.withDefaultArgs(args));
  }
}
