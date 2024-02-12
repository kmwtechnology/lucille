package com.kmwllc.lucille.ocr.stage;

import static org.bytedeco.leptonica.global.leptonica.pixRead;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import javax.imageio.ImageIO;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.leptonica.PIX;
import org.bytedeco.tesseract.TessBaseAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

public class ApplyOCR extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ApplyOCR.class);

  private final FormTemplate form;
  private final String lang;
  private transient TessBaseAPI api = null;
  private final String pathField;
  private final String pageField;

  public ApplyOCR(Config config) {
    super(config, new StageSpec().withOptionalProperties("page_field").withRequiredProperties("lang", "path_field").withRequiredParents("form"));
    
    form = ConfigBeanFactory.create(config.getConfig("form"), FormTemplate.class);
    lang = config.getString("lang");
    pathField = config.getString("path_field");
    pageField = ConfigUtils.getOrDefault(config, "page_field", null);
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


  public String ocr(BufferedImage image) throws IOException {
    // The tesseract api has some issues reading the image directly as a byte
    // array.
    // so for now... until that changes, we'll write a temp file to be ocr'd and
    String tempFilename = "tesseract." + UUID.randomUUID().toString() + ".png";
    File tempFile = new File("data", tempFilename);
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      ImageIO.write(image, "png", fos);
    }
    String result = ocr(tempFile.getAbsolutePath());
    tempFile.delete();
    return result;
  }

  public String ocr(String filename) throws IOException {
    try (PIX image = pixRead(filename)) {
      api.SetImage(image);
      //single column of text
      api.SetPageSegMode(4);
      //tell tesseract about the resolution of the rendered page so it doesn't have to guess
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

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if(!doc.has(pathField)) {
      return null;
    }
    int page = doc.has(pageField) ? doc.getInt(pageField) : 0;

    ArrayList<BufferedImage> images;
    Map<String, String> extracted;
    try {
      images = FormUtils.loadPdf(doc.getString(pathField));
      extracted = extractTemplate(images.get(page), form);
    } catch(IOException e) {
      log.warn("Error while extracting: {}", e);
      return null;
    }

    for(Map.Entry<String, String> entry: extracted.entrySet()) {
      doc.update(entry.getKey(), UpdateMode.OVERWRITE, entry.getValue());
    }

    return null;
  }

}
