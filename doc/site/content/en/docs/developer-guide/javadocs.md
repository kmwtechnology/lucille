---
title: Javadoc
weight: 70
description: >
  The published API reference for Lucille, plus the authoring standards for writing Javadoc on new components.
---

This page covers two things: where to find the published Javadoc for the Lucille API, and how to write Javadoc on new Connectors, Stages, and Indexers.

---

## Published API Reference

The generated Javadoc for `lucille-core` is published at:

**[javadoc.io/doc/com.kmwllc/lucille-core](https://javadoc.io/doc/com.kmwllc/lucille-core/latest/index.html)**

This covers all public classes, interfaces, and methods in the core library, including the `Document`, `Stage`, `Connector`, `Indexer`, and `Publisher` APIs.

---

## Javadoc Standards for Components

Every Connector, Stage, and Indexer must have a class-level Javadoc comment in this format.

**Rules:**

- Put a clear description before the `<p>` tag. This can be multiple sentences.
- After `<p>`, include the literal heading `Config Parameters -` followed by a `<ul>` list.
- Each `<li>` must follow the format: `name (Type, Required | Optional) : Description.`
- Use exact casing for `Required` and `Optional`.
- Escape generic type parameters: `List&lt;String&gt;`, `Map&lt;String, Object&gt;`.
- Do not add extra blank lines within the Javadoc block. Keep punctuation consistent.

**Template:**

```java
/**
 * Description of what this component does. This text can span multiple sentences
 * and be as long as needed, as long as it appears before the <p> tag.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>paramA (String, Required) : Description of paramA.</li>
 *   <li>paramB (Integer, Optional) : Description of paramB. Defaults to 10.</li>
 *   <li>flags (List&lt;String&gt;, Optional) : Description of flags.</li>
 *   <li>options (Map&lt;String, Object&gt;, Optional) : Description of options.</li>
 * </ul>
 */
```

**Example — a Stage:**

```java
/**
 * Renames fields on a Document according to a configured mapping. Source fields that are
 * absent on a given Document are silently skipped.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, String&gt;, Required) : Map of source field names to destination field names.</li>
 *   <li>updateMode (String, Optional) : How to handle existing destination fields. Defaults to "overwrite".</li>
 * </ul>
 */
public class RenameFields extends Stage {
```
