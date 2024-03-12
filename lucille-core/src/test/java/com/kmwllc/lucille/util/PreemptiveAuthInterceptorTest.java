package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.http.Consts;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PreemptiveAuthInterceptorTest {

  @Test
  public void testProcess() throws Exception {
    Credentials credentials = mock(Credentials.class);
    CredentialsProvider credsProvider = mock(CredentialsProvider.class);
    when(credsProvider.getCredentials(new AuthScope("foo", 1))).thenReturn(credentials);
    when(credsProvider.getCredentials(new AuthScope("bar", 1))).thenReturn(null);

    AuthState nullState = mock(AuthState.class);
    when(nullState.getAuthScheme()).thenReturn(null);
    HttpContext nullContext = mock(HttpContext.class);
    when(nullContext.getAttribute(HttpClientContext.TARGET_AUTH_STATE)).thenReturn(nullState);
    HttpHost targetHostFoo = new HttpHost("foo", 1);
    when(nullContext.getAttribute(HttpClientContext.HTTP_TARGET_HOST)).thenReturn(targetHostFoo);
    when(nullContext.getAttribute(HttpClientContext.CREDS_PROVIDER)).thenReturn(credsProvider);

    AuthState state = mock(AuthState.class);
    when(state.getAuthScheme()).thenReturn(new BasicScheme());
    HttpContext context = mock(HttpContext.class);
    when(context.getAttribute(HttpClientContext.TARGET_AUTH_STATE)).thenReturn(state);

    HttpContext errorContext = mock(HttpContext.class);
    when(errorContext.getAttribute(HttpClientContext.TARGET_AUTH_STATE)).thenReturn(nullState);
    HttpHost targetHostBar = new HttpHost("bar", 1);
    when(errorContext.getAttribute(HttpClientContext.HTTP_TARGET_HOST)).thenReturn(targetHostBar);
    when(errorContext.getAttribute(HttpClientContext.CREDS_PROVIDER)).thenReturn(credsProvider);

    HttpRequest request = mock(HttpRequest.class);

    new PreemptiveAuthInterceptor().process(request, context);
    new PreemptiveAuthInterceptor().process(request, nullContext);

    HttpException exception = assertThrows(HttpException.class, () -> {new PreemptiveAuthInterceptor().process(request, errorContext);});
    assertEquals("No credentials provided for preemptive authentication.", exception.getMessage());

    // since state returns a non-null auth scheme it should not have update called on it
    verify(state, times(0)).update(any(), any());

    ArgumentCaptor<BasicScheme> schemeCaptor = ArgumentCaptor.forClass(BasicScheme.class);
    ArgumentCaptor<Credentials> credentialCaptor = ArgumentCaptor.forClass(Credentials.class);

    // since nullState returns null for calls to getAuthScheme() update is called one
    verify(nullState, times(1)).update(schemeCaptor.capture(), credentialCaptor.capture());
    assertEquals(credentials, credentialCaptor.getValue());
    assertTrue(schemeCaptor.getValue() instanceof BasicScheme);
    assertEquals(Consts.ASCII, schemeCaptor.getValue().getCredentialsCharset());
  }
}
