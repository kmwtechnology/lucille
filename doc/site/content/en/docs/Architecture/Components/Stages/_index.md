---
title: Stages
date: 2025-06-06
description: A Stage performs a specific transformation on a Document.
---

## Lucille Stages

Stages are the building blocks of a Lucille pipeline. Each Stage performs a specific transformation on a Document. 

Lucille Stages should have JavaDocs that describe their purpose and the parameters acceptable in their Config. On this site,
you'll find more in-depth documentation for some more advanced / complex Lucille Stages. 

To configure a stage, you have to provide its class (under `class`) in its config. You can also specify a `name` for the Stage as well,
in addition to `conditions` and `conditionPolicy` (described below).

You'll also provide the parameters needed by the Stage as well. For example, the `AddRandomBoolean` Stage accepts two optional parameters -
`field_name` and `percent_true`. So, an `AddRandomBoolean` Config would look something like this:

```hocon
{
  name: "AddRandomBoolean-First"
  class: "com.kmwllc.lucille.stage.AddRandomBoolean"
  field_name: "rand_bool_1"
  percent_true: 65
}
```

#### Conditions

For any Stage, you can specify "conditions" in its Config, controlling when the Stage will process a Document. Each
condition has a required parameter, `fields`, and two optional parameters, `operator` and `values`.

* `fields` is a list of field names that will determine whether the Stage applies to a Document.

* `values` is a list of values that the conditional fields will be searched for. (If not specified, only the existence of fields is checked.)

* `operator` is either `"must"` or `"must_not"` (defaults to `"must"`).

In the root of the Stage's Config, you can also specify a `conditionPolicy` - either `"any"` or `"all"`, specifying whether
**any** or **all** of your conditions must be met for the Stage to process a Document. (Defaults to `"any"`.)

Let's say we are running the `Print` Stage, but we only want it to execute on a Document where `city = Boston` or `city = New York`.
Our Config for this Stage would look something like this:

```hocon
{
  name: "print-1"
  class: "com.kmwllc.lucille.stage.Print"
  conditions: [
    {
      fields: ["city"]
      values: ["Boston", "New York"]
    }
  ]
}
```