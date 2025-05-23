package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

/**
 * Applies OpenNLP NameFinder models of your choosing to a specified field of text on a Document. Extracts the detected names and
 * adds them to the Document.
 *
 * <p>Uses Apache OpenNLP's pretrained models.</p>
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>textField (String): The name of the field containing text you want to get names from.</li>
 *   <li>tokenizerPath (String): A path to the binaries of the tokenizer model you want to use.</li>
 *   <li>models (Map&lt;String, String&gt;): A map of "name types" to paths to OpenNLP model binaries. The keys for this map will become the field names containing the names extracted by that model.</li>
 * </ul>
 */
public class ApplyOpenNLPNameFinders extends Stage {

  private final String textField;
  private TokenizerME tokenizer;
  private final Map<String, Object> modelPathMap;
  private final Map<String, NameFinderME> finderMap;

  public ApplyOpenNLPNameFinders(Config config) {
    super(config, Spec.stage().withRequiredProperties("textField"));

    this.textField = config.getString("textField");
    this.modelPathMap = config.getConfig("models").root().unwrapped();
    this.finderMap = new HashMap<>();
  }

  @Override
  public void start() throws StageException {
    // try to load all the models and build a Map<String, NameFinderME>, throw exception if any errors occur
    try {
      this.tokenizer = new TokenizerME(new TokenizerModel(getClass().getResourceAsStream("/en-token.bin")));
    } catch (IOException e) {
      throw new StageException("Error initializing Tokenizer model: ", e);
    }


    try {
      for (Entry<String, Object> modelEntry : modelPathMap) {

      }
    }


    try {


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

    for (Map.Entry<String, NameFinderME> entry : finderMap.entrySet()) {
      Set<String> names = new HashSet<>();

      String modelKey = entry.getKey();
      NameFinderME finder = entry.getValue();

      Span[] spans = finder.find(tokens);
      for (Span span : spans) {
        StringBuilder entity = new StringBuilder();

        for (int i = span.getStart(); i < span.getEnd(); i++) {
          entity.append(tokens[i]);

          // append a space if it is not the final token
          if (i < span.getEnd() - 1) {
            entity.append(" ");
          }
        }

        names.add(entity.toString());
      }

      names.forEach(name -> doc.setOrAdd(modelKey, name));
    }

    return null;
  }
}