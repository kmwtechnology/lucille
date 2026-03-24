---
title: PromptOllama
date: 2025-06-06
description: Connect to Ollama Server and send a Document to an LLM for enrichment.
---

What if you could just, actually, put an LLM on everything?

### Ollama

Ollama allows you to run a variety of Large Language Models (LLMs) with minimal setup. You can also create custom models
using Modelfiles and system prompts. 

The `PromptOllama` Stage allows you to connect to a running instance of Ollama Server, which communicates with an LLM through a simple API.
The Stage sends part (or all) of a Document to the LLM for generic enrichment. You'll want to create a custom model (with a Modelfile)
or provide a System Prompt in the Stage Config that is tailored to your pipeline.

We _strongly_ recommend you have the LLM output only a JSON object for two main reasons: Firstly, LLMs tend to follow instructions better when 
instructed to do so. Secondly, Lucille can then parse the JSON response and fully integrate it into your Document. 

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
  hostURL: "http://localhost:9200"
  modelName: "fraud_detector"
  fields: ["email_text"]
}
```

* **System Prompt:** You can also just reference a specific LLM directly, and provide a system prompt in the Stage configuration.

```hocon
{
  name: "Ollama-Fraud"
  class: "com.kmwllc.lucille.stage.PromptOllama"
  hostURL: "http://localhost:9200"
  modelName: "gemma3"
  systemPrompt: "You are to read the text inside \"email_text\" and output a JSON object containing only one field, fraud, a boolean, representing whether the text contains evidence of fraud or not."
  fields: "email_text"
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

Lucille will then add all key-value pairs in this response JSON into your Document. So, the Document will become:
```json
{
  "id": "emails.csv-85",
  "run-id": "f9538992-5900-459a-90ce-2e8e1a85695c",
  "email_text": "Let's be sure to juice the numbers in our next quarterly earnings report.",
  "fraud": true
}
```

As you can see, `PromptOllama` is very versatile, and can be used to enrich your Documents in a _lot_ of ways.