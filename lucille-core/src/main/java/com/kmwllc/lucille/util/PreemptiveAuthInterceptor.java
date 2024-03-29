package com.kmwllc.lucille.util;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

/**
 * A custom request interceptor to enable preemptive authentication.
 */
public class PreemptiveAuthInterceptor implements HttpRequestInterceptor {

  // https://stackoverflow.com/questions/2014700/preemptive-basic-authentication-with-apache-httpclient-4
  // ^ link to more information on this PreemptiveAuthInterceptor
  @Override
  public void process(HttpRequest request, HttpContext context) throws HttpException {
    AuthState authState = (AuthState) context.getAttribute(HttpClientContext.TARGET_AUTH_STATE);
    if (authState.getAuthScheme() == null) {
      CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(HttpClientContext.CREDS_PROVIDER);
      HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
      Credentials credentials = credsProvider.getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
      if (credentials == null) {
        throw new HttpException("No credentials provided for preemptive authentication.");
      }
      authState.update(new BasicScheme(), credentials);
    }
  }
}
