---
title: Troubleshooting
date: 2024-10-15
description: A guide to some common issues and their resolution.
---

## Timeouts
Lucille has a default timeout specified in `runner.connectorTimeout` of 24 hours. You can override this timeout in the configuration file. This may be necessary in the case of very large or long-running jobs that may take more than 24 hours to complete. 