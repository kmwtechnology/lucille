---
title: PromptOllama
weight: 490
date: 2025-06-06
description: Connect to Ollama Server and send a Document to an LLM for enrichment.
---

`com.kmwllc.lucille.stage.PromptOllama`

What if you could just, actually, put an LLM on everything?

### Ollama

Ollama allows you to run a variety of Large Language Models (LLMs) with minimal setup. You can also create custom models
using Modelfiles and system prompts. 

The `PromptOllama` Stage allows you to connect to a running instance of Ollama Server, which communicates with an LLM through a simple API.
The Stage sends part (or all) of a Document to the LLM for generic enrichment. You'll want to create a custom model (with a Modelfile)
or provide a System Prompt in the Stage Config that is tailored to your pipeline.

We _strongly_ recommend you have the LLM output only a JSON object for two main reasons: Firstly, LLMs tend to follow instructions better when 
instructed to do so. Secondly, Lucille can then parse the JSON response and fully integrate it into your Document. 

If the response is a JSON object, each of its top-level keys is written to the Document according to `updateMode`, except reserved field names such as `id`, which are skipped. If the response is not a JSON object (it is invalid JSON, or valid JSON such as `true`, `42`, or an array), the raw text is written to the `ollamaResponse` field (always overwriting), unless `requireJSON` is `true`, in which case the document fails.

Each Document gets its own request, built from the `systemPrompt` and that Document alone, so no conversation state carries over between Documents.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `hostURL` | String | Yes | URL of the Ollama server, for example `http://localhost:11434`. |
| `modelName` | String | Yes | Name of the Ollama model to use. |
| `systemPrompt` | String | No | System prompt sent with each request. Not needed if your model's Modelfile already sets one. |
| `fields` | List\<String\> | No | Fields sent to the LLM, as a JSON object of field name to value. If empty or not set, the entire Document is sent. |
| `requireJSON` | Boolean | No | When `true`, the request asks Ollama for JSON output, and a response that is not a JSON object fails the document. When `false`, such a response is written to `ollamaResponse`. Default: `false`. |
| `updateMode` | String | No | How fields from a JSON response are written: `overwrite`, `append`, or `skip`. Does not apply to `ollamaResponse`. Default: `overwrite`. |
| `timeout` | Long | No | Request timeout in seconds. Default: `10`. Consider raising it when using multiple worker threads against a local Ollama server. |

##### Example

Let's say you are working with Documents which represent emails, and you want to monitor them for potential signs of fraud. Lucille doesn't
have a `DetectFraud` Stage (at time of writing), but you can use `PromptOllama` to add this information with an LLM. 

* **Modelfile:** Let's say you created a custom model, `fraud_detector`, in your instance of Ollama Server. As part of the modelfile,
you instruct the model to check the contents for fraud and output a JSON object containing just a boolean value (under `fraud`).
Your Stage would be configured like so:

```hocon
{
  name: "Ollama-Fraud"
  class: "com.kmwllc.lucille.stage.PromptOllama"
  hostURL: "http://localhost:11434"
  modelName: "fraud_detector"
  fields: ["email_text"]
}
```

* **System Prompt:** You can also just reference a specific LLM directly, and provide a system prompt in the Stage configuration.

```hocon
{
  name: "Ollama-Fraud"
  class: "com.kmwllc.lucille.stage.PromptOllama"
  hostURL: "http://localhost:11434"
  modelName: "gemma3"
  systemPrompt: """
    You are to read the text inside "email_text" and output a JSON object
    containing only one field, fraud, a boolean, representing whether the
    text contains evidence of fraud or not.
  """
  fields: ["email_text"]
}
```

Regardless of the approach you choose, the LLM will receive a request that looks like this:

```json
{
  "email_text": "Let's be sure to juice the numbers in our next quarterly earnings report."
}
```

(Since `fields: ["email_text"]`, any other fields on this Document are not part of the request.)

And the response from the LLM should look like this:
```json
{
  "fraud": true
}
```

Lucille will then add all key-value pairs in this response JSON into your Document, using `updateMode`. So, the Document will become:
```json
{
  "id": "emails.csv-85",
  "run_id": "f9538992-5900-459a-90ce-2e8e1a85695c",
  "email_text": "Let's be sure to juice the numbers in our next quarterly earnings report.",
  "fraud": true
}
```

As you can see, `PromptOllama` is very versatile, and can be used to enrich your Documents in a _lot_ of ways.