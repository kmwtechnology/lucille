---
title: Component Developer Guide
weight: 7
description: >
  Practical guidance for developers implementing new Connectors, Stages, and Indexers for Lucille.
---

This section covers everything you need to implement new Connectors, Stages, and Indexers for Lucille.

## Where Your Code Lives

The most common approach is to write your components in your own Java project and put the compiled JAR on the classpath when running Lucille. All approaches require you to reference the component's **fully qualified class name** (`class = "..."`) in the config.

**1. Use your own local code**

Put your classes anywhere in your own package — for example, `com.mycompany.ingest.MyStage`. Build your project to produce a JAR, then include it on the classpath alongside Lucille when running:

```bash
java -Dconfig.file=my-config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*:my-components.jar' \
  com.kmwllc.lucille.core.Runner
```

Reference your class in the config by its fully qualified name:

```hocon
stages: [
  {
    class: "com.mycompany.ingest.MyStage"
    myParam: "value"
  }
]
```

Lucille instantiates components reflectively using the `class` property. As long as the class is on the classpath and follows the expected constructor signature, it will work — there is no registration step or service loader.

**2. Contribute to lucille-core or create a plugin**

If your component is general-purpose and has no heavy dependencies, you can contribute it directly to `lucille-core`. If it depends on a large library, create a new module under `lucille-plugins/`. See the [Contributor Guide]({{< relref "docs/contributing" >}}) for project structure, build conventions, and how to submit a pull request.

