package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.InputStream;
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
 *   <li>tokenizerPath (String): A path to the binaries of the tokenizer model you want to use. Can be a classpath file or a local file.</li>
 *   <li>models (Map&lt;String, String&gt;): A map of "name types" to paths to OpenNLP model binaries. The keys for this map will become the field names containing the names extracted by that model. Can be a classpath file or a local file.</li>
 *   <li>confidenceThreshold (Double, optional): Minimum confidence threshold for entity extraction (0.0-1.0). Entities below this threshold will be filtered out. Default: 0.7</li>
 * </ul>
 */
public class ApplyOpenNLPNameFinders extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("textField", "tokenizerPath")
      .requiredParent("models", new TypeReference<Map<String, String>>(){})
      .optionalNumber("confidenceThreshold").build();

  private final String textField;
  private final String tokenizerPath;
  private final Map<String, Object> modelPathMap;
  private final double confidenceThreshold;
  private Map<String, NameFinderME> finderMap;
  private TokenizerME tokenizer;

  public ApplyOpenNLPNameFinders(Config config) {
    super(config);

    this.textField = config.getString("textField");
    this.tokenizerPath = config.getString("tokenizerPath");
    this.modelPathMap = config.getConfig("models").root().unwrapped();
    // Default confidence threshold of 0.7, can be overridden in config
    this.confidenceThreshold = config.hasPath("confidenceThreshold") ? 
        config.getDouble("confidenceThreshold") : 0.7;

    this.finderMap = new HashMap<>();
  }

  @Override
  public void start() throws StageException {
    try {
      InputStream modelInputStream = FileContentFetcher.getOneTimeInputStream(tokenizerPath);
      this.tokenizer = new TokenizerME(new TokenizerModel(modelInputStream));
    } catch (Exception e) {
      throw new StageException("Error initializing Tokenizer model.", e);
    }

    try {
      for (Entry<String, Object> modelEntry : modelPathMap.entrySet()) {
        if (!(modelEntry.getValue() instanceof String)) {
          throw new IllegalArgumentException("Model entry " + modelEntry.getKey() + " was set to a non-String value.");
        }

        String modelPath = (String) modelEntry.getValue();
        InputStream modelInputStream = FileContentFetcher.getOneTimeInputStream(modelPath);
        NameFinderME nameFinder = new NameFinderME(new TokenNameFinderModel(modelInputStream));

        finderMap.put(modelEntry.getKey(), nameFinder);
      }
    } catch (Exception e) {
      throw new StageException("Error initializing the NameFinder models.", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) {
    String text = doc.getString(textField);
    String[] tokens = tokenizer.tokenize(text);

    for (Map.Entry<String, NameFinderME> entry : finderMap.entrySet()) {
      Set<String> names = new HashSet<>();

      String modelKey = entry.getKey();
      NameFinderME finder = entry.getValue();

      Span[] spans = finder.find(tokens);
      for (Span span : spans) {
        // Only keep entities above confidence threshold
        if (span.getProb() >= confidenceThreshold) {
          StringBuilder entity = new StringBuilder();

          for (int i = span.getStart(); i < span.getEnd(); i++) {
            entity.append(tokens[i]);

            // appending a space if it is not the final token
            if (i < span.getEnd() - 1) {
              entity.append(" ");
            }
          }

          names.add(entity.toString());
        }
      }

      names.forEach(name -> doc.setOrAdd(modelKey, name));
    }

    return null;
  }
}