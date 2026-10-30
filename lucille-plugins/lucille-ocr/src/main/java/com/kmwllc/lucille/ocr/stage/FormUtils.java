package com.kmwllc.lucille.ocr.stage;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;

public class FormUtils {
  private FormUtils() {
  }

  // helper method to return a list of rendered pages as images
  public static List<BufferedImage> loadPdf(String filename) throws IOException {
    // Loading an existing PDF document
    File file = new File(filename);
    try (PDDocument document = PDDocument.load(file)) {
      return loadPdf(document);
    }
  }

  public static List<BufferedImage> loadPdf(InputStream stream) throws IOException {
    try (PDDocument document = PDDocument.load(stream)) {
      return loadPdf(document);
    }
  }

  private static List<BufferedImage> loadPdf(PDDocument document) throws IOException {
    List<BufferedImage> images = new ArrayList<>();
    // Instantiating the PDFRenderer class
    PDFRenderer renderer = new PDFRenderer(document);
    int numPages = document.getNumberOfPages();
    for (int i = 0; i < numPages; i++) {
      // Rendering an image from the PDF document
      BufferedImage image = renderer.renderImageWithDPI(i, ApplyOCR.SOURCE_RESOLUTION);
      images.add(image);
      // System.out.println("Image created");
    }
    return images;
  }

  public static BufferedImage cropImage(BufferedImage input, Rectangle roi) {
    BufferedImage subImg = input.getSubimage(roi.getX(), roi.getY(), roi.getWidth(), roi.getHeight());
    return subImg;
  }
}
