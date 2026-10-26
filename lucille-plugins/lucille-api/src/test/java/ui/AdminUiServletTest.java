package ui;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.ui.AdminUiServlet;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;

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

  @Test
  public void testServesRealAsset() throws Exception {
    when(request.getPathInfo()).thenReturn("/assets/app.js");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/javascript");
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("lucille admin ui test asset"));
  }

  @Test
  public void testServesCssWithCssContentType() throws Exception {
    when(request.getPathInfo()).thenReturn("/assets/app.css");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/css");
  }

  @Test
  public void testServesIndexForRoot() throws Exception {
    when(request.getPathInfo()).thenReturn("/");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/html; charset=UTF-8");
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("<div id=\"root\">"));
  }

  @Test
  public void testFallsBackToIndexForUnknownClientRoute() throws Exception {
    when(request.getPathInfo()).thenReturn("/configs/abc-123");

    servlet.service(getRequest(), response);

    verify(response).setContentType("text/html; charset=UTF-8");
    verify(response, never()).sendError(HttpServletResponse.SC_NOT_FOUND);
    assertTrue(body.toString(StandardCharsets.UTF_8).contains("<div id=\"root\">"));
  }

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
