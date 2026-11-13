package com.kmwllc.lucille.all;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.io.StringReader;
import java.io.StringWriter;
import org.bytedeco.javacpp.Loader;
import org.junit.Test;

/**
 * Guards against dependency-version conflicts that only appear when every plugin shares one classpath.
 */
public class ClasspathAlignmentTest {

  @XmlRootElement
  public static class Bean {
    public String name;
  }

  // Tika (jakarta JAXB 4.x) and Hadoop (javax JAXB 2.3.x) both ship a JAXB runtime; they must resolve to one stack.
  @Test
  public void testJaxbRoundTrip() throws Exception {
    JAXBContext context = JAXBContext.newInstance(Bean.class);
    Bean in = new Bean();
    in.name = "lucille";
    StringWriter xml = new StringWriter();
    context.createMarshaller().marshal(in, xml);
    Bean out = (Bean) context.createUnmarshaller().unmarshal(new StringReader(xml.toString()));
    assertEquals("lucille", out.name);
  }

  // lucille-ocr (tesseract) and lucille-video (javacv) must agree on the javacpp release train: every bytedeco jar's
  // version must end with the javacpp version (e.g. tesseract-5.5.1-1.5.12.jar, javacpp-1.5.12.jar).
  @Test
  public void testBytedecoJarsShareJavacppRelease() {
    String javacppJar = jarName(Loader.class);
    String release = javacppJar.replaceFirst("^javacpp-", "").replaceFirst("\\.jar$", "");
    assertTrue("unexpected javacpp jar " + javacppJar, javacppJar.startsWith("javacpp-") && !release.isEmpty());

    for (Class<?> c : new Class<?>[] {
        org.bytedeco.tesseract.TessBaseAPI.class, org.bytedeco.leptonica.PIX.class,
        org.bytedeco.ffmpeg.avcodec.AVCodec.class, org.bytedeco.javacv.FFmpegFrameGrabber.class}) {
      String jar = jarName(c);
      assertTrue(jar + " is not from javacpp release " + release, jar.endsWith("-" + release + ".jar"));
    }
  }

  // lucille-ocr and lucille-tika both use pdfbox; the pdfbox, pdfbox-io and fontbox jars must be one version, or Tika's
  // PDF parser fails with NoSuchFieldError.
  @Test
  public void testPdfboxJarsShareVersion() {
    String pdfboxJar = jarName(org.apache.pdfbox.pdmodel.PDDocument.class);
    String version = pdfboxJar.replaceFirst("^pdfbox-", "").replaceFirst("\\.jar$", "");
    assertTrue("unexpected pdfbox jar " + pdfboxJar, pdfboxJar.startsWith("pdfbox-") && version.startsWith("3."));

    assertEquals("pdfbox-io-" + version + ".jar", jarName(org.apache.pdfbox.io.RandomAccessRead.class));
    assertEquals("fontbox-" + version + ".jar", jarName(org.apache.fontbox.FontBoxFont.class));
  }

  private static String jarName(Class<?> c) {
    String path = c.getProtectionDomain().getCodeSource().getLocation().getPath();
    return path.substring(path.lastIndexOf('/') + 1);
  }
}
