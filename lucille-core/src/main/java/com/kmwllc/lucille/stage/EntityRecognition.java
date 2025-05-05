package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.EntityMentionsAnnotator;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Gets the names of people, organizations, and locations from a text field on a Document, and places the extracted names
 * into String lists on the Document.
 *
 * Config Parameters:
 * - textField (String): The name of the field with a long string of text that you want to get entity names from.
 */
public class EntityRecognition extends Stage {

  private final String textField;

  private final StanfordCoreNLP pipeline;

  public EntityRecognition(Config config) {
    super(config, Spec.stage().withRequiredProperties("textField"));

    this.textField = config.getString("textField");

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

    // Iterate over all sentences found
    List<CoreMap> sentences = textAnnotation.get(CoreAnnotations.SentencesAnnotation.class);
    for (CoreMap sentence : sentences) {
      for (CoreMap mention : sentence.get(CoreAnnotations.MentionsAnnotation.class)) {
        String type = mention.get(CoreAnnotations.EntityTypeAnnotation.class);

        if (type.equals("PERSON")) {
          doc.addToField("people", mention.toString());
        } else if (type.equals("ORGANIZATION")) {
          doc.addToField("organizations", mention.toString());
        } else if (type.equals("LOCATION")) {
          doc.addToField("locations", mention.toString());
        }
      }
    }

    return null;
  }
}
