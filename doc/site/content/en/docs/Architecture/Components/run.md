---
title: Runner
date: 2025-10-23
description: Component that manages a Lucille run, end-to-end. When you run Lucille at the command line in standalone mode, you are invoking the runner.
---

When invoked, the runner reads the configuration file and then begins a Lucille run by launching the appropriate connector(s) and publisher. The runner generates a `runId` per Lucille run and terminates based on messages sent by the Publisher.

What the runner invokes can be thought of is an end to end *Lucille Run.*

A Lucille Run is a sequence of connectors to be run, one after the other. Each connectors feeds to a specific pipeline. A run can include multiple connectors feeding multiple pipelines.









