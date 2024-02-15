package com.kmwllc.lucille.ocr.stage;

import static org.bytedeco.leptonica.global.leptonica.pixRead;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;
import org.apache.commons.io.FilenameUtils;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.leptonica.PIX;
import org.bytedeco.tesseract.TessBaseAPI;
import org.eclipse.parsson.JsonMergePatchImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import co.elastic.clients.json.JsonpMappingException;

public class ApplyOCR extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ApplyOCR.class);

  private final String lang;
  private transient TessBaseAPI api = null;
  private final String pathField;
  private final String byteArrayField;
  private final String pagesField;
  private final Map<Integer, String> pages;
  private final Map<String, FormTemplate> extractionTemplates;
  private final String extractAllDest;


  public ApplyOCR(Config config) throws StageException {
    super(config,
        new StageSpec()
            .withOptionalProperties("pages_field", "extraction_templates", "path_field", "byte_array_field", "extract_all_dest")
            .withRequiredProperties("lang").withOptionalParents("pages"));

    lang = config.getString("lang");
    pathField = ConfigUtils.getOrDefault(config, "path_field", null);
    byteArrayField = ConfigUtils.getOrDefault(config, "byte_array_field", null);
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

    if (pages != null && extractionTemplates == null) {
      throw new StageException("extraction_templates must be specified when pages is");
    }
    if (extractAllDest != null && extractionTemplates != null) {
      throw new StageException("extract_all_dest and extraction_templates can not both be specified");
    }

    initModel(lang);
  }

  private void initModel(String lang) {
    api = new TessBaseAPI();
    String tessData = "TesseractOcr";
    // load the models
    if (api.Init(tessData, lang) != 0) {
      System.out.println("Unable to load tesseract model.");
    }

  }

  private String ocr(BufferedImage image) {
    // The tesseract api has some issues reading the image directly as a byte
    // array.
    // so for now... until that changes, we'll write a temp file to be ocr'd and
    try {

      String tempFilename = "tesseract." + UUID.randomUUID().toString() + ".png";
      File tempFile = new File("data", tempFilename);
      try (FileOutputStream fos = new FileOutputStream(tempFile)) {
        ImageIO.write(image, "png", fos);
      }
      String result = ocr(tempFile.getAbsolutePath());
      tempFile.delete();
      return result;
    } catch (IOException e) {
      log.warn("IOException encountered while doing extraction: {}", e);
      return null;
    }
  }

  private String ocr(String filename) {
    try (PIX image = pixRead(filename)) {
      api.SetImage(image);
      // single column of text
      api.SetPageSegMode(4);
      // tell tesseract about the resolution of the rendered page so it doesn't have to guess
      api.SetSourceResolution(300);
      // Get OCR result
      try (BytePointer outText = api.GetUTF8Text()) {
        // log.info("OCR output:\n" + ret);
        // Destroy used object and release memory
        return outText.getString();
      }
    }
  }

  public Map<String, String> extractTemplate(BufferedImage page, FormTemplate template) throws IOException {
    Map<String, String> results = new LinkedHashMap<>();
    for (Rectangle r : template.getRegions()) {
      BufferedImage roiCrop = FormUtils.cropImage(page, r);
      String result = ocr(roiCrop);
      results.put(r.getLabel(), result);
    }
    return results;
  }

  private void extractPagesToDoc(Map<Integer, String> pages, ArrayList<BufferedImage> images, Document doc) {

    for (Map.Entry<Integer, String> entry : pages.entrySet()) {
      Map<String, String> extractedText;
      FormTemplate template = extractionTemplates.get(entry.getValue());
      if (template == null) {
        log.warn("No template with name: {}. Skipping this template...", entry.getValue());
        continue;
      }

      try {
        extractedText = extractTemplate(images.get(entry.getKey()), template);
      } catch (IOException e) {
        log.warn("Error while extracting: {}. Skipping template: {}...", e, template.getName());
        continue;
      } catch (IndexOutOfBoundsException e) {
        log.warn("Page: {}, does not exist", )
      }

      extractedText.entrySet().stream().forEach((region) -> doc.update(region.getKey(), UpdateMode.OVERWRITE, region.getValue()));;
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(pathField)) {
      return null;
    }
    String path = doc.getString(pathField);
    String type = FilenameUtils.getExtension(path);


    if (type == "pdf") {
      ArrayList<BufferedImage> images = new ArrayList<>();
      try {
        images = FormUtils.loadPdf(doc.getString(pathField));
      } catch (IOException e) {
        log.warn("Error while loading pdf: {}. Skipping this document...", e);
        return null;
      }

      // do full extraction for pdfs
      if (extractAllDest != null) {
        doc.update(extractAllDest, UpdateMode.OVERWRITE,
            images.stream().map(image -> ocr(image)).filter(Objects::nonNull).collect(Collectors.toList()).toArray(new String[0]));
        return null;
      } else {
        // extract templates that are statically defined
        if (pages != null) {
          extractPagesToDoc(pages, images, doc);
        }

        // extract template that are dynamically defined on the document
        if (pagesField != null && doc.has(pagesField)) {
          ObjectMapper mapper = new ObjectMapper();
          Map<Integer, String> pages = new LinkedHashMap<>();
          try {
            Map<String, Object> map = mapper.readValue(doc.getString(pagesField), new TypeReference<>() {});
            for(Map.Entry<String, Object> entry : map.entrySet()) {
              pages.put(Integer.parseInt(entry.getKey()), (String) entry.getValue());
            }
          } catch (Exception e) {
            log.warn("Invalid json at field: {}. Skipping dynamic template extraction for this document.\n{}", pagesField, e);
            return null;
          }
          extractPagesToDoc(pages, images, doc);
        }
      }
    } else {
      if (extractAllDest == null) {
        log.warn("Cannot do form extraction for non-pdf files. extract_all_dest field must be specified");
        return null;
      }
      doc.update(extractAllDest, UpdateMode.OVERWRITE, ocr(pathField));
    }
    return null;
  }

}
