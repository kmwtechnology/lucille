# lucille-logging

Lucille's runtime logging backend.

The jar contains two resources:

- `lucille-log4j2.xml`: the default Log4j2 configuration
- `EcsLayout.json`: the JSON layout template used by the config

The configuration is deliberately not named `log4j2.xml`, so Log4j2 does not auto-discover it just because
the jar is on the classpath. Select it explicitly with the Log4j2 system property:

```
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml ...
```

To use your own configuration, point `-Dlog4j2.configurationFile` at your file instead.
