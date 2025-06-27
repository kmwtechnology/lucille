---
title: Config Validation
date: 2025-06-27
description: How Lucille validates Configs, and what developers need to know.
---

## Config Validation

Lucille components (like Stages, Indexers, and Connectors) each take in a set of specific arguments to configure the component correctly.
Sometimes, certain properties are required - like the `pathsToStorage` for your `FileConnector` traversal, or the `path` for your
`CSVIndexer`. Other properties are optional / do not always have to be specified.

For these components, developers must declare a `Spec` which defines the properties that are required or optional. They must
also declare what type each property is (number, boolean, string, etc.). For example, the `SequenceConnector` requires you
to specify the `numDocs` you want to create, and optionally, the number you want IDs to `startWith`. So, the `Spec` looks like this:

```java
public static final Spec SPEC = Spec.connector()
      .requiredNumber("numDocs")
      .optionalNumber("startWith");
```

## Creating a Spec

Lucille is designed to access Specs reflectively. If you build a Stage/Indexer/Connector/File Handler, you need to declare a `public static Spec` 
named, `SPEC` (exactly) in order to use your component. Failure to do so will not result in a _compile-time_ error. However, you will not be able
to instantiate your component - even in unit tests - as the reflective access (which takes place in the super / abstract class) will always fail.

When you declare the `public static Spec SPEC`, you'll want to call the appropriate `Spec` method which provides appropriate
default arguments for your component. For example, if you are building a Stage, you should call `Spec.stage()`, which allows
the config to include `name`, `class`, `conditions`, and `conditionPolicy`. 

## Parent / Child Validation

Some configs include properties which are objects, containing _even more_ properties.

## Why Validate?

1. Striving to validate a Config once and getting a list of any potential errors
2. Prevent typos from ruining your pipeline