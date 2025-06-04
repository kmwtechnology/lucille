package com.kmwllc.lucille.ocr.stage;

import static org.bytedeco.leptonica.global.leptonica.pixRead;

import com.kmwllc.lucille.core.spec.Spec;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;
import org.apache.commons.io.FilenameUtils;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.leptonica.PIX;
import org.bytedeco.tesseract.TessBaseAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

/**
 * Applies optical character recognition to images. Additionally supports form extraction using 
 * templates. See README for a more detailed explanation.
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>lang</b> (String) : The language to use for OCR.
 * </p>
 * <p>
 * <b>path_field</b> (String, Optional) : Field on document containing a path to the image used for extraction. The 
 * extension is used to determine if the image is a pdf. If the document does not contain this field it is skipped.
 *  </p>
 * <p>
 * <b>extract_all_dest</b> (String, Optional) : If this field is specified, ocr is applied to the entire image 
 * and the result is stored in this field. For pdfs, ocr is applied to each page seperately and the field becomes multi-valued
 * <p>
 * <b>extraction_templates</b> (List&lt;FormTemplate&gt;, Optional) : A list of form templates defined as such:
 * </p>
 * <pre>
 * {
 *   name: "w2",
 *   regions: [
 *    {
 *      x: 0,
 *      y: 0,
 *      width: 100,
 *      height: 100,
 *      dest: "field1"
 *    },
 *    {
 *      x: 100,
 *      y: 200,
 *      width: 200,
 *      height: 300,
 *      dest: "field2"
 *    }
 *  ]
 * }
 * </pre>
 * The name field is the name of the template and regions is a list of rectangular portions of the pages to extract. They are each extracted to the field specified by `dest`
 * and appended to a multivalued field if there is already something there.
 *  <p>
 * <b>pages</b> (Map&lt;Integer,String&gt;, Optional) : A map from page numbers to template names allowing a user to statically specify 
 * which types of forms are on which pages. Page 0 is used to indicate the one and only page on non-pdf files.
 * </p>
 * <p>
 * <b>pages_field</b> (String, Optional) : Field on document containing a JSON string used for dynamically applying templates to documents.
 * This allows a user to specify on the document itself which types of forms the document is holding and on which pages. The JSON should be a map 
 * from page numbers to template names. If a page appears in both the static and dynamic mapping, the dynamic one takes precedence.
 * </p>
 */
public class ApplyOCR extends Stage {

  public static final String TEMP_DIR = "lucille-ocr-temp";
  public static final int SOURCE_RESOLUTION = 300;
  private static final Logger log = LoggerFactory.getLogger(ApplyOCR.class);

  private final String lang;
  private transient TessBaseAPI api = null;
  private final String pathField;
  private final String pagesField;
  private final Map<Integer, String> pages;
  private final Map<String, FormTemplate> extractionTemplates;
  private final String extractAllDest;


  public ApplyOCR(Config config) throws StageException {
    super(config, Spec.stage().withOptionalProperties("pages_field", "extraction_templates", "extract_all_dest")
        .withRequiredProperties("lang", "path_field")
        .withOptionalParentNames("pages"));

    lang = config.getString("lang");
    pathField = ConfigUtils.getOrDefault(config, "path_field", null);
    pagesField = ConfigUtils.getOrDefault(config, "pages_field", null);
    extractAllDest = ConfigUtils.getOrDefault(config, "extract_all_dest", null);

    if (config.hasPath("extraction_templates")) {
      extractionTemplates = new LinkedHashMap<>();
      List<FormTemplate> temp = config.getConfigList("extraction_templates").stream()
          .map((c) -> ConfigBeanFactory.create(c, FormTemplate.class)).collect(Collectors.toList());
      for (FormTemplate template : temp) {
        extractionTemplates.put(template.getName(), template);
      }
    } else {
      extractionTemplates = null;
    }

    if (config.hasPath("pages")) {
      Map<String, Object> temp = config.getConfig("pages").root().unwrapped();
      pages = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : temp.entrySet()) {
        pages.put(Integer.parseInt(entry.getKey()), (String) entry.getValue());
      }
    } else {
      pages = null;
    }

    if ((pages != null || pagesField != null) && extractionTemplates == null) {
      throw new StageException("extraction_templates must be specified when pages or pages_field is specified");
    }
  }

  @Override
  public void start() throws StageException {
    api = new TessBaseAPI();
    String tessData = "TesseractOcr";
    // load the models
    if (api.Init(tessData, lang) != 0) {
      throw new StageException(String.format("Unable to load tesseract model: %s", lang));
    }
  }

  private String applyOcr(BufferedImage image) {
    File tempFile = null;
    try {
      File dir = new File(TEMP_DIR);
      dir.mkdir();
      tempFile = File.createTempFile("tesseract.", ".png", dir);
      try (FileOutputStream fos = new FileOutputStream(tempFile)) {
        ImageIO.write(image, "png", fos);
      }
      String result = applyOcr(tempFile.getAbsolutePath());
      return result;
    } catch (IOException e) {
      log.warn("IOException encountered while doing extraction: {}", e);
      return null;
    } finally {
      try {
        if (tempFile != null) {
          tempFile.delete();
        }
      } catch (Exception e) {
        log.warn("Error deleting temp file: {}", e);
      }
    }
  }

  private String applyOcr(String filename) throws FileNotFoundException {
    try (PIX image = pixRead(filename)) {
      if (image == null) {
        throw new FileNotFoundException(String.format("%s cannot be opened", filename));
      }
      api.SetImage(image);
      // single column of text
      api.SetPageSegMode(4);
      // tell tesseract about the resolution of the rendered page so it doesn't have to guess
      api.SetSourceResolution(SOURCE_RESOLUTION);
      // Get OCR result
      try (BytePointer outText = api.GetUTF8Text()) {
        // Destroy used object and release memory
        return outText.getString();
      }
    }
  }

  private Map<String, List<String>> extractTemplate(BufferedImage page, FormTemplate template) throws IOException {
    Map<String, List<String>> results = new LinkedHashMap<>();
    for (Rectangle r : template.getRegions()) {
      if (!results.containsKey(r.getDest())) {
        results.put(r.getDest(), new ArrayList<>());
      }
      BufferedImage roiCrop = FormUtils.cropImage(page, r);
      String result = applyOcr(roiCrop);
      results.get(r.getDest()).add(result);
    }
    return results;
  }

  private void extractPagesToDoc(Map<Integer, String> pages, ArrayList<BufferedImage> images, Document doc) {

    for (Map.Entry<Integer, String> entry : pages.entrySet()) {
      Map<String, List<String>> extractedText;
      FormTemplate template = extractionTemplates.get(entry.getValue());
      if (template == null) {
        log.warn("No template with name: {}. Skipping this template...", entry.getValue());
        continue;
      }

      try {
        extractedText = extractTemplate(images.get(entry.getKey()), template);
      } catch (IOException e) {
        log.warn("Skipping template: {}. Error while extracting: {}", template.getName(), e);
        continue;
      } catch (IndexOutOfBoundsException e) {
        log.warn("Page: {}, does not exist on document with ID: {}. Skipping template.", entry.getKey(), doc.getId());
        continue;
      }

      extractedText.entrySet().stream()
          .forEach((region) -> doc.update(region.getKey(), UpdateMode.APPEND, region.getValue().toArray(new String[0])));
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(pathField)) {
      log.warn("Document with id: {} does not have field: {}", doc.getId(), pathField);
      return null;
    }
    String path = doc.getString(pathField);
    String type = FilenameUtils.getExtension(path);


    // doing pdf extraction
    ArrayList<BufferedImage> images = new ArrayList<>();
    try {
      if (type.equals("pdf")) {
        images = FormUtils.loadPdf(path);
      } else {
        if (pagesField != null || pages != null) {
          images.add(ImageIO.read(Files.newInputStream(Paths.get(path))));
        }
      }
    } catch (IOException e) {
      log.warn("Error while loading file: {}. Skipping this document...", e);
      return null;
    }

    // do full extraction
    if (extractAllDest != null) {
      if (type.equals("pdf")) {
        doc.update(extractAllDest, UpdateMode.OVERWRITE,
            images.stream().map(image -> applyOcr(image)).filter(Objects::nonNull).collect(Collectors.toList()).toArray(new String[0]));
      } else {
        try {
          doc.update(extractAllDest, UpdateMode.OVERWRITE, applyOcr(path));
        } catch (FileNotFoundException e) {
          log.warn("File not found: {}", e);
        }
      }
    }

    Map<Integer, String> templatesToBeApplied = new LinkedHashMap<>();
    // add templates that are statically defined
    if (pages != null) {
      templatesToBeApplied.putAll(pages);
    }

    // add templates that are dynamically defined on the document (overwriting static ones)
    if (pagesField != null && doc.has(pagesField)) {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> json = null;
      try {
        json = mapper.readValue(doc.getString(pagesField), new TypeReference<>() {});
      } catch (Exception e) {
        log.warn("Invalid json at field: {}. Skipping dynamic template extraction for this document.\n{}", pagesField, e);
      }

      if (json != null) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
          try {
            templatesToBeApplied.put(Integer.parseInt(entry.getKey()), (String) entry.getValue());
          } catch (NumberFormatException e) {
            log.warn("Invalid page number: {}", entry.getKey());
            continue;
          }
        }
      }
    }

    extractPagesToDoc(templatesToBeApplied, images, doc);

    return null;
  }
}
