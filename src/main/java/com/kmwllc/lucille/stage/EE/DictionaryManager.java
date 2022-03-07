package com.kmwllc.lucille.stage.EE;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Controls interaction with a Dictionary to be used for Entity Extraction.
 * <p/>
 * Created by matt on 4/19/17.
 */
public interface DictionaryManager {
  /**
   * Loads the dictionary into memory from an InputStream of data
   * @param in The stream to build the dictionary from
   * @throws IOException If the stream can't be read from
   */
  void loadDictionary(InputStream in) throws IOException;

  /**
   * Answers with the String composed of the specified tokens has an entry in the dictionary
   * @param tokens List of String tokens
   * @return Whether the String composed from the tokens is in the dictionary
   * @throws IOException If the dictionary can't be read from
   */
  boolean hasTokens(List<String> tokens) throws IOException;

  /**
   * Retrieves the Entity found in the dictionary under the String composed from the
   * list of specified tokens.
   * @param tokens List of String tokens to be assembled for lookup
   * @return The entity found
   * @throws IOException If the dictionary can't be read from
   */
  EntityInfo getEntity(List<String> tokens) throws IOException;

  /**
   * Retrieves list of all Entity's found in a given input.  This list will contains the
   * payloads of the entities if they are defined in the dictionary.  If not, the list will
   * hold the matched terms instead.
   * @param input Input to extract entities from
   * @param doNested Extract nested entities
   * @param doOverlap Extract overlapping entities
   * @return List of all Entities found in given input
   * @throws IOException If the dictionary can't be read
   */
  List<String> findEntityStrings(String input, boolean doNested, boolean doOverlap)
    throws IOException;

  /**
   * Retrieves all entities found in a given input.  The resulting list includes the EntityInfo
   * that was found along with a Range object which indicates where in the input the match was
   * found
   * @param input Input to extract entities from
   * @param doNested Extract nested entities
   * @param doOverlap Extract overlapping entities
   * @return List of Entity Annotations (range + entity) found in the input
   * @throws IOException If the dictionary can't be read
   */
  List<EntityAnnotation> findEntities(String input, boolean doNested, boolean doOverlap)
    throws IOException;
}
