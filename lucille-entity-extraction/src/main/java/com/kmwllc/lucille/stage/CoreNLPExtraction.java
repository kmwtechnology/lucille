package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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
public class CoreNLPExtraction extends Stage {

  private final String textField;

  private StanfordCoreNLP pipeline;

  public CoreNLPExtraction(Config config) {
    super(config, Spec.stage().withRequiredProperties("textField"));

    this.textField = config.getString("textField");
  }

  // Initializing the model takes a few seconds, so we do it in the start() method.
  @Override
  public void start() throws StageException {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,entitymentions");
    // only have "PERSON", "ORGANIZATION", or "LOCATION".
    // For example, don't output "CITY" instead of the more generic "LOCATION" on a city name
    props.setProperty("ner.applyFineGrained", "false");
    pipeline = new StanfordCoreNLP(props);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String text = doc.getString(textField);

    Annotation textAnnotation = new Annotation(text);
    pipeline.annotate(textAnnotation);

    // Building sets to prevent duplicates
    Set<String> people = new HashSet<>();
    Set<String> organizations = new HashSet<>();
    Set<String> locations = new HashSet<>();

    // Iterate over all sentences found
    List<CoreMap> sentences = textAnnotation.get(CoreAnnotations.SentencesAnnotation.class);
    for (CoreMap sentence : sentences) {
      for (CoreMap mention : sentence.get(CoreAnnotations.MentionsAnnotation.class)) {
        String type = mention.get(CoreAnnotations.EntityTypeAnnotation.class);

        if (type.equals("PERSON")) {
          people.add(mention.toString());
        } else if (type.equals("ORGANIZATION")) {
          organizations.add(mention.toString());
        } else if (type.equals("LOCATION")) {
          locations.add(mention.toString());
        }
      }
    }

    // trimming whitespace as the model has a tendency to add a trailing space to entity names
    people.forEach(person -> doc.addToField("coreNLP_people", person.trim()));
    organizations.forEach(organization -> doc.addToField("coreNLP_organizations", organization.trim()));
    locations.forEach(location -> doc.addToField("coreNLP_locations", location.trim()));

    return null;
  }
}
