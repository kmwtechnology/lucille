# Salesforce Contacts to OpenSearch

This example crawls Contact records from a Salesforce Developer Edition org and indexes each record in OpenSearch. The connector is `SalesforceConnector`. The config is `conf/contacts.conf`. The index name is `contacts`.

The connector logs in with the OAuth client-credentials flow. A Developer Edition org created in Summer ’26 or later cannot use the username-password flow: **Setup → OAuth and OpenID Connect Settings → Allow OAuth Username-Password Flows** is grayed out, and Salesforce will not accept a password grant. Leave `username` and `password` out of the config.

## Requirements

- An External Client App in the org, configured as below. New orgs create an External Client App rather than a Connected App.
- OpenSearch listening at the URL in `conf/contacts.conf`. A local install uses a certificate that is not signed by a public CA, so the config sets `acceptInvalidCert: true`.
- `OPENSEARCH_URL` and `OPENSEARCH_INDEX` override those defaults when they are set.

## Developer org setup

Sign up for a Developer Edition org and log in. The host in the browser address bar, before `/lightning`, is the org’s My Domain URL. It looks like `https://your-domain.develop.my.salesforce.com`. Client credentials cannot use `https://login.salesforce.com`.

### Create the app

1. In **Setup**, open **External Client App Manager** and create an app.
2. Enable OAuth. The form requires a callback URL. The client-credentials grant never opens it; `https://login.salesforce.com/services/oauth2/callback` is enough.
3. Under **Selected OAuth Scopes**, add **Manage user data via APIs (api)**. Client credentials ignores **Full access (full)**, **Perform requests at any time (refresh_token)**, and **OpenID**. If none of the selected scopes are valid for this flow, the token request fails with `invalid_grant: no valid scopes defined`.
4. Enable the client-credentials flow on the app’s OAuth settings and save. Salesforce can take a few minutes to apply the change.
5. Open the app’s action menu and choose **Edit Policies**. This is a different page from the OAuth settings that show the consumer key. Select **Enable Client Credentials Flow** again, and set the execution user (**Run As**) to your Salesforce **Username**. Choose the user from the lookup. Typing the name without selecting the record leaves the field empty, and the token request then fails with `invalid_grant: no client credentials user enabled`. **Permitted Users** does not set this user.
6. Save the policies and wait a few minutes.
7. On the app’s OAuth settings, open **Consumer Key and Secret**. The consumer key is `clientId`. The consumer secret is `clientSecret`.

The execution user is the identity the crawl runs as. A Developer Edition administrator can read Contact.

### Point the config at the org

In `conf/contacts.conf`, set `loginUrl` to the My Domain URL and set the consumer key and secret. Do not set `username` or `password`. With those two values absent, the connector uses the client-credentials grant.

```hocon
loginUrl: "https://your-domain.develop.my.salesforce.com"
clientId: ${SALESFORCE_CLIENT_ID}
clientSecret: ${SALESFORCE_CLIENT_SECRET}

sobjects: ["Contact"]
```

```powershell
$env:SALESFORCE_CLIENT_ID = "<consumer key>"
$env:SALESFORCE_CLIENT_SECRET = "<consumer secret>"
```

The key and secret can be written directly in the config instead of read from the environment.

## Run

From the repository root, package this example and the modules it depends on:

```powershell
mvn -pl lucille-examples/lucille-salesforce-example -am package -DskipTests
```

From `lucille-examples/lucille-salesforce-example`:

```powershell
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml -Dconfig.file=conf/contacts.conf -cp "target/lib/*" com.kmwllc.lucille.core.Runner
```

`RunSalesforceIngest` launches the same config from the IDE.

Each Contact becomes one document. The document id is the Salesforce `Id` with the prefix `sf-`. The field `salesforce_sobject` is `Contact`.
