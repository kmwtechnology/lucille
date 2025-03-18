# OpenSearch Authentication Issue Support Ticket

## Issue Description
We're experiencing persistent authentication issues when trying to connect to our OpenSearch instance at `https://34.139.11.141:9200` from the Lucille ingestion pipeline. Despite providing valid credentials (username: `admin`, password: `StrongPassword123!`), we consistently receive a 401 Unauthorized error.

## Environment Details
- **Platform**: macOS 15.3.2
- **Java Version**: OpenJDK 21.0.5+11-LTS (Temurin)
- **Lucille Version**: 0.5.4-SNAPSHOT
- **OpenSearch Client Version**: 2.11.1 (based on classpath)

## Troubleshooting Steps Already Taken
1. **Basic Authentication**: Passed username/password directly through the configuration
2. **Environment Variables**: Set OpenSearch credentials via environment variables
3. **Base64 Authentication**: Created a Base64 encoded auth token and passed it as an Authorization header
4. **Multiple Auth Formats**: Configured authentication in multiple formats (standard, security-specific, node-specific)
5. **SSL Configuration**: Disabled SSL verification to eliminate certificate issues (using `opensearch.acceptInvalidCert=true`, `opensearch.ssl.verification=false`, etc.)
6. **Direct Curl Tests**: Tested connection with curl using both `-u` flag and explicit Authorization header
7. **Java System Properties**: Set multiple forms of authentication properties for the Java client
8. **Proxy Configuration**: Explicitly disabled proxy settings to ensure direct connection
9. **Debug Logging**: Enabled HTTP client debug logging to view authentication attempts

## Configuration Details
We're using a complex file connector configuration with specific file inclusion/exclusion patterns:

```json
"excludePatterns": [
    "**/target/**", "**/.idea/**", "**/.vscode/**", "**/.github/**", "**/.svn/**", 
    "**/.hg/**", "**/.tox/**", "**/.pytest_cache/**", "**/.DS_Store/**", 
    "node_modules/", "dist/", "*.py[cod]", "*.iml", "*.project", ".settings", 
    ".classpath", "TEST*.xml", "Thumbs.db", "*.mp4", "*.tiff", "*.avi", "*.flv", 
    "*.mov", "*.wmv", "**/*.class", "**/*.jar", "**/pom.xml", "**/*.log", "**/test/**/*"
],
"includeSuffixes": [
    ".md", ".html", ".java", ".properties", ".xml", ".conf", ".csv"
]
```

We've also confirmed that `opensearch.acceptInvalidCert` is set to `true` in the Java system properties:

```
_JAVA_OPTIONS: -Dopensearch.config.file=/var/folders/ww/rjw59f354sj8wc8s3rjgm8n80000gn/T/tmp.XXMZf19wNj
-Djavax.net.ssl.trustStore=/dev/null
-Djavax.net.ssl.trustStorePassword=password
-Djavax.net.ssl.trustStoreType=JKS
-Dsun.net.http.allowRestrictedHeaders=true
-Dhttps.protocols=TLSv1.2
-Dorg.apache.http.auth.protocol.http.enabled=true
-Dorg.apache.http.auth.protocol.https.enabled=true
-Dopensearch.client.authentication.enabled=true
-Dopensearch.acceptInvalidCert=true
```

## Error Log
```
25/03/18 17:11:57 5cafb14b-fe1a-48e1-9c36-0c0603f1c6f4  ERROR OpenSearchIndexer: Couldn't ping OpenSearch 
java.io.IOException: Unauthorized access
        at org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport.extractAndWrapCause(ApacheHttpClient5Transport.java:1150)
        at org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport.performRequest(ApacheHttpClient5Transport.java:158)
        at org.opensearch.client.opensearch.OpenSearchClient.ping(OpenSearchClient.java:1083)
        at com.kmwllc.lucille.indexer.OpenSearchIndexer.validateConnection(OpenSearchIndexer.java:86)

Caused by: org.opensearch.client.transport.TransportException: Unauthorized access
        at org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport.prepareResponse(ApacheHttpClient5Transport.java:499)
        
Caused by: org.opensearch.client.transport.httpclient5.ResponseException: method [HEAD], host [https://34.139.11.141:9200], URI [/], status line [HTTP/1.1 401 Unauthorized]
```

## OpenSearch Configuration
Our OpenSearch instance appears to be using basic authentication. We've confirmed the credentials work when used with curl:
```bash
curl -k -u "admin:StrongPassword123!" -X GET "https://34.139.11.141:9200"
```

## Additional Observations
- We've tried setting both environment variables and Java system properties with the same credentials
- The curl command succeeds but the Java client fails with the same credentials
- We've verified that the properties are being picked up by the JVM (visible in the debug output)
- Multiple authentication formats have been attempted (basic auth, header-based auth, property-based auth)
- The client seems to be making a HEAD request to the root endpoint (/) rather than using a POST or GET method

## Questions
1. Is there a specific authentication format required by the OpenSearch client library that differs from standard HTTP Basic Auth?
2. Could there be an issue with how the OpenSearch client is handling credentials passed as system properties?
3. Is there a configuration in OpenSearch server that might be rejecting Java client connections despite accepting the same credentials via curl?
4. Are there any known issues with the OpenSearch Java client (version 2.11.1) related to authentication?
5. Does the OpenSearch client's use of HEAD requests for ping operations require different authentication handling compared to GET/POST operations?

## Request
We need assistance resolving this authentication issue to enable our Lucille ingestion pipeline to connect to our OpenSearch instance. Any guidance on the correct authentication configuration or potential causes of this issue would be greatly appreciated.

## Contact Information
- **Name**: Kevin
- **Email**: [please provide email]
- **Organization**: [please provide organization]
- **Project**: Lucille OpenSearch Vector Example
