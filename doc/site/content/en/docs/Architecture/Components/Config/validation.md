---
title: Config Validation
date: 2025-06-27
description: How Lucille validates Configs - and what developers need to know.
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

## Declaring a Spec

Lucille is designed to access Specs reflectively. If you build a Stage/Indexer/Connector/File Handler, you need to declare a `public static Spec` 
named `SPEC` (exactly). Failure to do so will not result in a _compile-time_ error. However, you will not be able
to instantiate your component - even in unit tests - as the reflective access (which takes place in the super / abstract class) will always fail.

When you declare the `public static Spec SPEC`, you'll want to call the appropriate `Spec` method which provides appropriate
default arguments for your component. For example, if you are building a Stage, you should call `Spec.stage()`, which allows
the config to include `name`, `class`, `conditions`, and `conditionPolicy`. 

## Lists and Objects

Validating a list / object is a bit tricky. When you declare a required / optional list or object in a Config, you can either
provide:

1. A `TypeReference` describing what the unwrapped List/Object should deserialize/cast to.
2. A `Spec`, for a list, or a `ParentSpec`, for an object, describing the valid properties. (Use a `Spec` for a list when you need a `List<Config>` with specific structure. For example, `Stage` conditions.)

## Parent / Child Validation

Some configs include properties which are objects, containing _even more_ properties. For example, in the `FileConnector`, you
can specify `fileOptions` - which includes a variety of additional arguments, like `getFileContent`, `handleArchivedFiles`, and more. 
This is defined in a `ParentSpec`. A `ParentSpec` has a name (the key the config is held under) and has its own required/optional properties.
The `fileOptions` `ParentSpec` is:

```java
Spec.parent("fileOptions")
  .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
  .optionalString("moveToAfterProcessing", "moveToErrorFolder");
```

A `ParentSpec` can be either required or optional. When the parent is present, its properties will be validated against this `ParentSpec`.

There will be times that you can't know what the field names would be in advance. For example, a field mapping of some kind.
In this case, you should pass in a `TypeReference` describing what type the unwrapped `ConfigObject` should deserialize/cast to.
For example, if you want a field mapping of Strings to Strings, you'd pass in `new TypeReference<Map<String, String>>(){}`.
In general, you should use a `Spec` when you know field names, and a `TypeReference` when you don't.

## Why Validate?

Obviously, you will get an error if you call `config.getString("field")` when `"field"` is not specified. However, additional validation
on Configs is still useful/necessary for two primary reasons:

1. **Command-line utility**

We want to allow the command-line validation utility to provide a comprehensive list of a Config's errors. As such, Lucille has to
validate the config before a Stage/Connector/Indexer begins accessing properties and potentially throwing a `ConfigException`.

2. **Prevent typos from ruining your pipeline**

A mistyped field name could have massive ripple effects throughout your pipeline. As such, each Stage/Connector/Indexer needs to
have a specific set of legal Config properties, so Exceptions can be raised for unknown or unrecognized properties.