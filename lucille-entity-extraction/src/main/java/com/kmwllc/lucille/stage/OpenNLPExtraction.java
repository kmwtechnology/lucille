package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

/**
 * Gets the names of people, organizations, and locations from a text field on a Document, and places the extracted names
 * into String lists on the Document. The names will be placed into "people", "organizations", and "locations", which will be lists.
 * The fields will not be present if no names are found.
 *
 * <p>Uses Apache OpenNLP's pretrained models.</p>
 *
 * <p> Config Parameters:
 * <p> - textField (String): The name of the field with a long string of text that you want to get entity names from.
 */
public class OpenNLPExtraction extends Stage {

  private final String textField;
  private TokenizerME tokenizer;
  private final Map<String, NameFinderME> finderMap;

  public OpenNLPExtraction(Config config) {
    super(config, Spec.stage().withRequiredProperties("textField"));

    this.textField = config.getString("textField");
    this.finderMap = new HashMap<>();
  }

  @Override
  public void start() throws StageException {
    try {
      this.tokenizer = new TokenizerME(new TokenizerModel(getClass().getResourceAsStream("/en-token.bin")));

      finderMap.put("PERSON", new NameFinderME(new TokenNameFinderModel(getClass().getResourceAsStream("/en-ner-person.bin"))));
      finderMap.put("ORGANIZATION", new NameFinderME(new TokenNameFinderModel(getClass().getResourceAsStream("/en-ner-organization.bin"))));
      finderMap.put("LOCATION", new NameFinderME(new TokenNameFinderModel(getClass().getResourceAsStream("/en-ner-location.bin"))));
    } catch (Exception e) {
      throw new StageException("Error initializing an OpenNLP model.", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String text = doc.getString(textField);

    String[] tokens = tokenizer.tokenize(text);

    Set<String> people = new HashSet<>();
    Set<String> organizations = new HashSet<>();
    Set<String> locations = new HashSet<>();

    for (Map.Entry<String, NameFinderME> entry : finderMap.entrySet()) {
      String type = entry.getKey();
      NameFinderME finder = entry.getValue();

      Span[] spans = finder.find(tokens);
      for (Span span : spans) {
        StringBuilder entity = new StringBuilder();
        for (int i = span.getStart(); i < span.getEnd(); i++) {
          entity.append(tokens[i]).append(" ");
        }

        if (type.equals("PERSON")) {
          people.add(entity.toString());
        } else if (type.equals("ORGANIZATION")) {
          organizations.add(entity.toString());
        } else if (type.equals("LOCATION")) {
          locations.add(entity.toString());
        }
      }
    }

    people.forEach(person -> doc.addToField("openNLP_people", person.trim()));
    organizations.forEach(organization -> doc.addToField("openNLP_organizations", organization.trim()));
    locations.forEach(location -> doc.addToField("openNLP_locations", location.trim()));

    return null;
  }
}