package com.kmwllc.lucille.ocr.stage;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class FormUtils {
  // helper method to return a list of rendered pages as images
  public static ArrayList<BufferedImage> loadPdf(String filename) throws IOException {
    // Loading an existing PDF document
    File file = new File(filename);
    try (PDDocument document = PDDocument.load(file)) {
      return loadPdf(document);
    }
  }

  public static ArrayList<BufferedImage> loadPdf(InputStream stream) throws IOException {
    try (PDDocument document = PDDocument.load(stream)) {
      return loadPdf(document);
    }
  }

  private static ArrayList<BufferedImage> loadPdf(PDDocument document) throws IOException {
    ArrayList<BufferedImage> images = new ArrayList<>();
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
