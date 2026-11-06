package com.kmwllc.lucille.ui;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;

// Tests AdminUiServlet against fixture assets: serving real files with the right content type,
// SPA fallback to index.html for unknown client routes, and refusing path traversal.
public class AdminUiServletTest {

  private final AdminUiServlet servlet = new AdminUiServlet();
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ByteArrayOutputStream body;

  @Before
  public void setUp() throws IOException {
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    body = new ByteArrayOutputStream();
    when(response.getOutputStream()).thenReturn(new CapturingServletOutputStream(body));
  }

  // A request for a real bundled file returns its bytes with the right content type.
  @Test
  public void testServesRealAsset() throws Exception {
    when(request.getPathInfo()).thenReturn("/assets/app.js");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/javascript");
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("lucille admin ui test asset"));
  }

  // A .css asset is served as text/css (content type derives from the extension).
  @Test
  public void testServesCssWithCssContentType() throws Exception {
    when(request.getPathInfo()).thenReturn("/assets/app.css");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/css");
  }

  // The root path serves index.html (the SPA entry point).
  @Test
  public void testServesIndexForRoot() throws Exception {
    when(request.getPathInfo()).thenReturn("/");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/html; charset=UTF-8");
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("<div id=\"root\">"));
  }

  // An unknown path (a client-side route) falls back to index.html instead of 404,
  // so deep links and refreshes let React Router resolve the route.
  @Test
  public void testFallsBackToIndexForUnknownClientRoute() throws Exception {
    when(request.getPathInfo()).thenReturn("/configs/abc-123");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/html; charset=UTF-8");
    verify(response, never()).sendError(HttpServletResponse.SC_NOT_FOUND);
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("<div id=\"root\">"));
  }

  // A "../" traversal attempt cannot escape admin/; it is treated as an unknown route.
  @Test
  public void testRejectsPathTraversalByFallingBackToIndex() throws Exception {
    when(request.getPathInfo()).thenReturn("/../../secret");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/html; charset=UTF-8");
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("<div id=\"root\">"));
  }

  private HttpServletRequest getRequest() {
    when(request.getMethod()).thenReturn("GET");
    when(request.getProtocol()).thenReturn("HTTP/1.1");
    return request;
  }

  private static final class CapturingServletOutputStream extends ServletOutputStream {
    private final ByteArrayOutputStream delegate;

    CapturingServletOutputStream(ByteArrayOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int b) {
      delegate.write(b);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {
    }
  }
}
