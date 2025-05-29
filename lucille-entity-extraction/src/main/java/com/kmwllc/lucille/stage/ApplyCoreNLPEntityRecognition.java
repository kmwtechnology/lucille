package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * <p> Applies CoreNLP's named entity recognition models to text from a Document, placing the extracted entities of different types
 * into lists on the processed Document. Uses Stanford CoreNLP's pretrained models. Extracts 12 different types of names by default,
 * increased to 24 when <code>fineGrained</code> is set to true.
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>textField (String): The name of the field with a long string of text that you want to get entity names from.</li>
 *   <li>fineGrained (Boolean, Optional): Whether to use CoreNLP's "fine-grained" mode or not. This results in more specific name types (like CITY, COUNTRY, etc. instead of just LOCATION),
 *   but is known to degrade performance. Defaults to false.</li>
 *   <li>nameTypes (List&lt;String&gt;, Optional): The types of names you want to retain. For example, if you only want person and location names, specify <code>[PERSON, LOCATION]</code>.
 *   Must match the name types specified by CoreNLP (but is case-insensitive). Defaults to adding all name types to the Document.</li>
 * </ul>
 *
 * @see <a href="https://stanfordnlp.github.io/CoreNLP/ner.html">CoreNLP: Named Entity Recognition</a>
 */
public class ApplyCoreNLPEntityRecognition extends Stage {

  private final String textField;
  private final boolean fineGrained;
  private final List<String> nameTypes;

  private StanfordCoreNLP pipeline;

  public ApplyCoreNLPEntityRecognition(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("textField")
        .withOptionalProperties("fineGrained", "nameTypes"));

    this.textField = config.getString("textField");
    this.fineGrained = ConfigUtils.getOrDefault(config, "fineGrained", false);

    if (config.hasPath("nameTypes")) {
      // storing lowercased version of all the strings
      this.nameTypes = config.getStringList("nameTypes").stream().map(String::toLowerCase).toList();

      if (nameTypes.isEmpty()) {
        throw new IllegalArgumentException("nameTypes cannot be empty.");
      }
    } else {
      nameTypes = null;
    }
  }

  @Override
  public void start() {
    Properties props = new Properties();
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,entitymentions,regexner");

    props.setProperty("ner.applyFineGrained", String.valueOf(fineGrained));

    pipeline = new StanfordCoreNLP(props);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) {
    String text = doc.getString(textField);
    Annotation textAnnotation = new Annotation(text);
    pipeline.annotate(textAnnotation);

    List<CoreMap> sentences = textAnnotation.get(CoreAnnotations.SentencesAnnotation.class);

    for (CoreMap sentence : sentences) {
      for (CoreMap mention : sentence.get(CoreAnnotations.MentionsAnnotation.class)) {
        String type = mention.get(CoreAnnotations.EntityTypeAnnotation.class);

        // name types get lowercased in constructor
        if (nameTypes == null || nameTypes.contains(type.toLowerCase())) {
          doc.addToField(type, mention.toString());
        }
      }
    }

    return null;
  }
}
