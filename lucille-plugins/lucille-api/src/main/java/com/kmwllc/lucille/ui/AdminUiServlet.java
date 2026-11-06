package com.kmwllc.lucille.ui;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.http.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves the bundled React/Vite admin UI from the classpath at {@code /admin}, falling back to
 * {@code index.html} for unmatched paths so client-side routes resolve. Served without
 * authentication; access control is a separate ticket.
 */
public class AdminUiServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(AdminUiServlet.class);

  private static final String RESOURCE_ROOT = "admin";

  private static final String INDEX_FILE = "index.html";

  /**
   * Constructs an AdminUiServlet.
   */
  public AdminUiServlet() {
    super();
  }

  /**
   * Serves the asset matching the request path, or {@code index.html} when none matches.
   * @param req the request
   * @param resp the response
   * @throws IOException if writing the response fails
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String relativePath = normalize(req.getPathInfo());

    String resourcePath = relativePath.isEmpty() ? null : RESOURCE_ROOT + "/" + relativePath;
    if (resourcePath != null && resourceExists(resourcePath)) {
      serve(resourcePath, resp);
    } else {
      serve(RESOURCE_ROOT + "/" + INDEX_FILE, resp);
    }
  }

  /**
   * Returns a safe root-relative resource path, empty for the root or any traversal attempt.
   * @param pathInfo the servlet path info, which may be null
   * @return a cleaned relative path, empty when the root was requested
   */
  private static String normalize(String pathInfo) {
    if (pathInfo == null || pathInfo.isEmpty() || "/".equals(pathInfo)) {
      return "";
    }
    String trimmed = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
    if (trimmed.contains("..")) {
      return "";
    }
    return trimmed;
  }

  /**
   * Whether a classpath resource exists for the given path.
   * @param resourcePath the classpath resource path
   * @return true if the resource is present
   */
  private boolean resourceExists(String resourcePath) {
    return Thread.currentThread().getContextClassLoader().getResource(resourcePath) != null
        || getClass().getClassLoader().getResource(resourcePath) != null;
  }

  /**
   * Streams the given classpath resource to the response with a best-effort content type.
   * @param resourcePath the classpath resource path
   * @param resp the response
   * @throws IOException if the resource cannot be read or written
   */
  private void serve(String resourcePath, HttpServletResponse resp) throws IOException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream in = loader == null ? null : loader.getResourceAsStream(resourcePath);
    if (in == null) {
      in = getClass().getClassLoader().getResourceAsStream(resourcePath);
    }

    if (in == null) {
      log.warn("Admin UI resource not found on classpath: {}", resourcePath);
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    try (InputStream resource = in) {
      resp.setContentType(contentTypeFor(resourcePath));
      try (OutputStream out = resp.getOutputStream()) {
        resource.transferTo(out);
      }
    }
  }

  /**
   * Returns the content type for the given resource path using Jetty's MIME table, with an explicit
   * UTF-8 charset for HTML and a binary default for unknown extensions.
   * @param resourcePath the classpath resource path
   * @return a MIME type, defaulting to {@code application/octet-stream}
   */
  private static String contentTypeFor(String resourcePath) {
    String mime = MimeTypes.DEFAULTS.getMimeByExtension(resourcePath);
    if (mime == null) {
      return "application/octet-stream";
    }
    if (MimeTypes.Type.TEXT_HTML.is(mime)) {
      return mime + "; charset=" + StandardCharsets.UTF_8.name();
    }
    return mime;
  }
}
