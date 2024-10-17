---
title: Setup & Standards
date: 2024-10-15
description: >
  A short lead description about this content page. It can be **bold** or _italic_ and can be split over multiple paragraphs.
categories: [Examples]
tags: [test, sample, docs]
---

## Coding Standards


## Local Developer Setup
### Prerequisite(s):
- IntelliJ application installed on machine
- Java project 

### Setting up Google Code Formatting Scheme
- Make sure that Intellij is open
- Go to the following link: [styleguide/intellij-java-google-style.xml at gh-pages · google/styleguide](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml)
- Download the .xml file
- Open the file in an editor of your choice
- Navigate to the <option …> tag with name ‘Right Margin’ and edit the value to be 132 (it should default as 100)
- Save the file
- In Intellij IDEA, navigate to Settings | Preferences → Code Style → Editor → Java
- Click on the gear icon on the right panel and drill down to the option Import Scheme and then to Intellij IDEA Code Style XML
- In the file explorer that opens, navigate to where you stored the aforementioned .xml file we downloaded
- After selecting the file, you should see a pop-up allowing you to name the scheme; select a name and click ‘Okay’
- Click ‘Apply’ in the Settings panel
- Restart the IDE; You can use the ‘Reformat Code’ option to apply the plug-in on your code

### Excluding Non-Java Files
Assuming that we don’t want to auto-format non-java files via a directory level ‘Reformat Code’ option, we need to exclude all other files from being reformatted
- Navigate to Settings | Preferences in Intellij IDEA
- Navigate to Editor → Code Style
- Click on the tab on the right window labeled ‘Formatter’
- In the ‘Do Not Format' text box, paste the following and click ‘Apply'

    ```*.{yml,xml,md,json,yaml,jsonl,sql}```
- A restart of Intellij may be required to see changes

This method may prove to be too complicated, especially when new file types are added to the codebase, therefore, consider the following, simpler method instead:
- When clicking on ‘Reformat Code’ at the directory level, a window will pop up
- Under the filter sections in the window, select the ‘File Mask(s)’ option and set the value to ‘*.java’
- This will INCLUDE all .java files in your reformatting 

 

### Eclipse Users

[Eclipse import conf .xml files](https://stackoverflow.com/questions/10432538/eclipse-import-conf-xml-files)

The linked post details some useful information for how Eclipse users can use the same .xml for their code formatting on Eclipse IDE.