---
title: Developing New Stages
weight: 25
date: 2024-10-28
description: >
  The basics of how to develop stages for Lucille.
---

## Introduction
This guide covers the basics of how to develop a new stage for Lucille and what some of its required and optional components are. After reading this, you should be able to start development with a good foundation on how testing works, how configuration is handled, and what features the Stage class affords us. 


## Prerequisite(s)
- An up-to-date version of Lucille that has been appropriately installed 

- Please refer to **Quick Start Guides - Lucille Local Mode** for more info on how to set up Lucille

- Understanding of Java programming language

- Understanding of Lucille 

## Developing a Stage:
*Note: Angle brackets (<…>) are used to show placeholder information that is meant to be replaced by you when using the code snippets.*
1. Create a new Java class underneath the following directory in Lucille; name should be in PascalCase
   -  ```lucille/lucille-core/src/main/java/com/kmw/lucille/stage/```
2. The new class should extend the Stage abstract class that can be found at the following directory in Lucille
   - ```lucille/lucille-core/src/main/java/com/kmw/lucille/core/```
   - The class declaration should look like the following
      ```public class <StageName> extends Stage {```
3. Create a constructor, your constructor should take in a config variable of type Config
   - The constructor declaration should look similar to this
```public <StageName>(Config config) {```
4. Next, call super() to reference the protected super constructor from the Stage class
   - We want to provide this constructor with the aforementioned config, but also with the names of any required or optional parameters / parents that we want to make configurable via the config
   - Note that the properties should be in camelCase
   - Example provided below:
```
super(config, new StageSpec()
  .withRequiredProperties("<requiredProperty>")
  .withOptionalProperties("<optionalProperty>")
  .withRequiredParents("<requiredParent>"));
```
5. Define instance variables that correlate to config properties you wish to request from the user (both required and optional)
   - The following code shows examples of common patterns used to extract config parameters; reference the Config code for more methods

```config.getConfig("<nameOfProperty>").root().unwrapped(); // for required properties```

```config.hasPath("<nameOfProperty>") ? config.getInt("<nameOfProperty>") : <defaultValue>;```

```ConfigUtils.getOrDefault(config, "<nameOfProperty>", <defaultValue>;```

6. Add the abstract method processDocument to your class
   - This method is where we want to make changes to fields on the document and potentially create child documents 
   - This method should return null assuming we are not intending to generate child documents
     - Reference the Javadoc in the Stage class for more information on how to support this functionality

7. Add appropriate comments to explain any important code in the class and add Javadoc before the class declaration
   - Javadoc should explain the behaviour of this Stage and also should list config parameters, their types, whether they are optional, and a short description

## Unit Testing:
Lucille uses JUnit as its testing framework, please refer to JUnit best practices when making tests. 

1. Create a new Java class underneath the following directory in Lucille; name should be the same as the Stage’s name, with ```Test``` appended to the end

   - ```lucille/lucille-core/src/test/java/com/kmw/lucille/stage/```
2. Create a new directory underneath the following directory in Lucille: name should same as the testing class' name

   - ```lucille/lucille-core/src/test/resources/```

   - Underneath this directory create a new file called config.conf; this will be an example config that will be used in our test class; you can create more for further testing

3. The following code snippet can be used to create a new Stage with the provided config name

```StageFactory.of(<StageName>.class).get("<StageTestName>/config.conf");```

4. The following code snippet will process a given Document; reference the Document class for more information
```s.processDocument(d); // where s is the Stage and d is the Document```
 

The following are standards for testing in Lucille:

- There should be at least one unit test for a Stage

- Recommended to aim for 80% code coverage, but not a hard cutoff

- When testing services, use mocks and spys

- Tests should test notable exceptions thrown by Stage class code

- Tests should cover all logical aspects of a Stage’s function

- Tests should be deterministic

- Code coverage should not only encapsulate aspects of lucille-core but also modules

## Extra Stage Resources:
- The ```Stage``` class has both the ```start``` and ```stop``` method; both are helpful for when we want to set up or tear down objects 

- Lucille also has a ```StageUtils``` class that has some methods that may prove useful in development