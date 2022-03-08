package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.stage.EE.EntityAnnotation;
import com.kmwllc.lucille.stage.EE.EntityInfo;
import com.kmwllc.lucille.stage.EE.FSTDictionaryManager;
import com.kmwllc.lucille.stage.EE.FSTDictionaryManagerFactory;
import com.kmwllc.lucille.util.Range;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;

public class FSTDictionaryManagerTest {
  private final String AUTHOR = "author";
  private final String ATHLETE = "athlete";
  private final String CITY = "city";
  private final String STATE = "state";
  private final String INVENTOR = "inventor";
  private final String GEORGE_WASHINGTON = "george washington";
  private final String PAUL_GEORGE = "paul george";
  private final String GEORGE_WASHINGTON_CARVER = "george washington carver";
  private final String WASHINGTON_IRVING = "washington irving";
  private final String WASHINGTON = "washington";

  private FSTDictionaryManager dm;
  private InputStream in;
  private InputStream in2;

  @Before
  public void before() throws FileNotFoundException {
    dm = FSTDictionaryManagerFactory.get().createDefault();
    in = new FileInputStream("src/test/resources/ExtractEntitiesTest/test-dict.csv");
    in2 = new FileInputStream("src/test/resources/AlternateExtractEntitiesTest/dictionary.txt");
  }

  @Test
  public void testLoadDictionary() {
    try {
      dm.loadDictionary(in);
      // Loaded w/o barfing
      assertTrue(true);
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testHasTokens() throws IOException {
    dm.loadDictionary(in);
    assertTrue(dm.hasTokens(newArrayList("george")));
    assertTrue(dm.hasTokens(newArrayList("george", "washington")));
    assertTrue(dm.hasTokens(newArrayList("george", "washington", "carver")));
    assertFalse(dm.hasTokens(newArrayList("george", "washington", "bridge")));
  }

  @Test
  public void testGetEntity() throws IOException {
    dm.loadDictionary(in);
    EntityInfo ei = dm.getEntity(newArrayList("george", "washington"));
    assertEquals(GEORGE_WASHINGTON, ei.getTerm());
    assertEquals(Collections.emptyList(), ei.getPayloads());
    ei = dm.getEntity(newArrayList("george", "washington", "carver"));
    assertEquals(GEORGE_WASHINGTON_CARVER, ei.getTerm());
    assertEquals(INVENTOR, ei.getPayloads().get(0));
    assertNull(dm.getEntity(newArrayList("george")));
  }

  @Test
  public void testFindEntities() throws IOException {
    dm.loadDictionary(in);
    String input = "in attendance were paul george, washington irving and others";
    List<EntityAnnotation> ea;

    ea = dm.findEntities(input, false, false);
    assertEquals(2, ea.size());
    assertEquals(new EntityAnnotation(
      new Range(3, 5), new EntityInfo(PAUL_GEORGE, newArrayList(ATHLETE))), ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(5, 7), new EntityInfo(WASHINGTON_IRVING, newArrayList(AUTHOR))), ea.get(1));

    ea = dm.findEntities(input, true, false);
    assertEquals(3, ea.size());
    assertEquals(new EntityAnnotation(
      new Range(3, 5), new EntityInfo(PAUL_GEORGE, newArrayList(ATHLETE))), ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(5, 6), new EntityInfo(WASHINGTON, newArrayList(CITY, STATE))), ea.get(1));
    assertEquals(new EntityAnnotation(
      new Range(5, 7), new EntityInfo(WASHINGTON_IRVING, newArrayList(AUTHOR))), ea.get(2));

    ea = dm.findEntities(input, false, true);
    assertEquals(3, ea.size());
    assertEquals(new EntityAnnotation(
      new Range(3, 5), new EntityInfo(PAUL_GEORGE, newArrayList(ATHLETE))), ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(4, 6), new EntityInfo(GEORGE_WASHINGTON, newArrayList())), ea.get(1));
    assertEquals(new EntityAnnotation(
      new Range(5, 7), new EntityInfo(WASHINGTON_IRVING, newArrayList(AUTHOR))), ea.get(2));

    ea = dm.findEntities(input, true, true);
    assertEquals(4, ea.size());
    assertEquals(new EntityAnnotation(
      new Range(3, 5), new EntityInfo(PAUL_GEORGE, newArrayList(ATHLETE))), ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(4, 6), new EntityInfo(GEORGE_WASHINGTON, newArrayList())), ea.get(1));
    assertEquals(new EntityAnnotation(
      new Range(5, 6), new EntityInfo(WASHINGTON, newArrayList(CITY, STATE))), ea.get(2));
    assertEquals(new EntityAnnotation(
      new Range(5, 7), new EntityInfo(WASHINGTON_IRVING, newArrayList(AUTHOR))), ea.get(3));


    input = "george washington carver was cool";
    ea = dm.findEntities(input, true, true);
    assertEquals(3, ea.size());
    assertEquals(new EntityAnnotation(
        new Range(0, 3), new EntityInfo(GEORGE_WASHINGTON_CARVER, newArrayList(INVENTOR))),
      ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(0, 2), new EntityInfo(GEORGE_WASHINGTON, newArrayList())), ea.get(1));
    assertEquals(new EntityAnnotation(
      new Range(1, 2), new EntityInfo(WASHINGTON, newArrayList(CITY, STATE))), ea.get(2));

    ea = dm.findEntities(input, false, true);
    assertEquals(1, ea.size());
    assertEquals(new EntityAnnotation(
        new Range(0, 3), new EntityInfo(GEORGE_WASHINGTON_CARVER, newArrayList(INVENTOR))),
      ea.get(0));


    input = "here comes george washington carver";
    ea = dm.findEntities(input, true, true);
    assertEquals(3, ea.size());
    assertEquals(new EntityAnnotation(
        new Range(2, 5), new EntityInfo(GEORGE_WASHINGTON_CARVER, newArrayList(INVENTOR))),
      ea.get(0));
    assertEquals(new EntityAnnotation(
      new Range(2, 4), new EntityInfo(GEORGE_WASHINGTON, newArrayList())), ea.get(1));
    assertEquals(new EntityAnnotation(
      new Range(3, 4), new EntityInfo(WASHINGTON, newArrayList(CITY, STATE))), ea.get(2));

    ea = dm.findEntities(input, false, false);
    assertEquals(1, ea.size());
    assertEquals(new EntityAnnotation(
        new Range(2, 5), new EntityInfo(GEORGE_WASHINGTON_CARVER, newArrayList(INVENTOR))),
      ea.get(0));
  }

  @Test
  public void testFindEntityStrings() throws IOException {
    dm.loadDictionary(in);

    String input = "paul george, washington irving and others were in attendance";
    List<String> entities = dm.findEntityStrings(input, false, false);
    assertEquals(2, entities.size());
    //assertThat(entities).contains(ATHLETE, AUTHOR);

    entities = dm.findEntityStrings(input, true, false);
    assertEquals(4, entities.size());
    //assertThat(entities).contains(ATHLETE, AUTHOR, CITY, STATE);

    entities = dm.findEntityStrings(input, false, true);
    assertEquals(3, entities.size());
    //assertThat(entities).contains(ATHLETE, GEORGE_WASHINGTON, AUTHOR);

    entities = dm.findEntityStrings(input, true, true);
    assertEquals(5, entities.size());
   // assertThat(entities).contains(ATHLETE, GEORGE_WASHINGTON, CITY, STATE, AUTHOR);

    input = "george washington carver was cool";
    entities = dm.findEntityStrings(input, true, true);
    assertEquals(4, entities.size());
    //assertThat(entities).contains(INVENTOR, GEORGE_WASHINGTON, CITY, STATE);

    entities = dm.findEntityStrings(input, false, true);
    assertEquals(1, entities.size());
    //assertThat(entities).contains(INVENTOR);

    input = "here comes george washington carver";
    entities = dm.findEntityStrings(input, true, true);
    assertEquals(4, entities.size());
    //assertThat(entities).contains(INVENTOR, GEORGE_WASHINGTON, CITY, STATE);

    entities = dm.findEntityStrings(input, false, false);
    assertEquals(1, entities.size());
    //assertThat(entities).contains(INVENTOR);


    input = "I live in the united states.";
    entities = dm.findEntityStrings(input, false, false);
    assertEquals(1, entities.size());
    //assertThat(entities).contains(INVENTOR, GEORGE_WASHINGTON, CITY, STATE);
  }


  @Test
  public void testMe() throws IOException {
    dm.loadDictionary(in2);

    String input = "United States";
    List<String> entities = dm.findEntityStrings(input, false, false);
    List<String> e = new ArrayList<>();
    e.add("United States");
    assertTrue(dm.hasTokens(e));
    assertEquals(1, entities.size());
    //assertThat(entities).contains(ATHLETE, AUTHOR);

  }
}

