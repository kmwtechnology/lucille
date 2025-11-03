---
title: Installation
weight: 15
date: 2025-10-31
description: >
  A guide to installing Lucille locally.
---

## Prerequisites

To build and run Lucille from source, you need:

* **Java 17+ JDK** (not just a JRE)
* **Maven** (recent version)

## Java Setup (JDK 17+ Required)

**Important:** Before running any Lucille commands, make sure **`JAVA_HOME` points to a JDK 17+** (not just a JRE) **and** that **`$JAVA_HOME/bin` is on your `PATH`** (or `%JAVA_HOME%\bin` on Windows). Maven and the `java` launcher rely on this.

### Verify Java

```bash
java -version
```

You should see **version 17** (or newer). If it’s missing or older than 17 install a JDK 17+ using **one** of the options below.

### Install Options

**Package manager**

* **macOS (Homebrew)**
    ```bash
    brew install openjdk@17
    ```
* **Windows (Chocolatey)**
    ```bash
    choco install microsoft-openjdk17
    ```
  
**Vendor installer**

* Download a JDK 17+ installer from a vendor such as **Oracle JDK**.
* Run the installer, then set `JAVA_HOME` as shown below.

### Set `JAVA_HOME` and `PATH`

**macOS**

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
```

**Windows**

* Open **System Properties**, **Environment Variables**.
* Create/Edit **JAVA_HOME** and point it to your JDK folder.
* Edit **Path** and add `%JAVA_HOME%\bin` above other Java entries.

## Maven Setup

```bash
mvn -v
```

You should see a recent Maven version and your Java home. If `mvn` is not found, install Maven using **one** of the options below.

### Install Options

**Package manager**

* **macOS (Homebrew)**
    ```bash
    brew install maven
    ```
* **Windows (Chocolatey)**
    ```bash
    choco install maven
    ```
  
**Binary installer**

* Download the binary zip/tar for **Apache Maven** from the official website.
* Add Maven's `bin/` to your `PATH`.

**macOS**

```bash
export PATH="<maven-dir>/bin:$PATH"
```

**Windows**

* Open **System Properties**, **Environment Variables**.
* Edit **Path** and add `<maven-dir>/bin`.

## Clone the Repository

```bash
git clone https://github.com/kmwtechnology/lucille.git
```

## Build Lucille

```bash
cd lucille
mvn clean install
```

This compiles all modules and produces build artifacts under each module’s `target/` folder.