# Optical Character Recognition Stage
This stage uses [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) to extract text from images and pdfs. It additionally supports form extraction using user defined templates. 

## Usage 
There are 3 ways in which the stage can be used all of which can be used simultaneously as defined in the config:
1. Full extraction 
2. Static form extraction
3. Dynamic form extraction

### Full Extraction 
To enable full extraction add a `extract_all_dest` property to the config. For non-pdf files (determined by the file extension) the stage will extract all text present to this field. 
For pdf files each page will have extraction applied separately and the results will be stored in a multi-valued field. NOTE: This overwrites what is at `extract_all_dest` if it already exists. 

### Static Form Extraction
Static form extraction allows a user to statically (in the config) specify form templates and the pages they should be applied. To enable this, include an `extraction_templates` field containing the templates and 
a `pages` field containing a map from page numbers to the name of the template to be applied (for non-pdf files a page number of 0 should be used to refer to the entire image). Templates themselves take the following form: 
```json 
  {
    name: "w2",
    regions: [
     {
       x: 0,
       y: 0,
       width: 100,
       height: 100,
       dest: "field1"
     },
     {
       x: 100,
       y: 200,
       width: 200,
       height: 300,
       dest: "field2"
     }
   ]
  }
```
The `name` property contains the name of the form and is used to refer to the template. The `regions` property contains an arbitrarily long list of rectangles whose interiors are extracted. The `dest` property within
them is where the text is extracted to. If duplicate `dest` values are used, subsequent extractions are appended to the field rather than overwriting it (this can be within one template or across multiple). NOTE: all coordinates and distances are measured in pixels.
Additionally, if a page or template does not exist, this is logged, but subsequent page template mappings are still applied. Duplicate page entries in the map overwrites previous ones.

### Dynamic Form Extraction 
This is identical to static form extraction but allows the documents themselves to specify which forms they contain and on which pages. To enable, include a `pages_field` property in the config which stores the field
in which the dynamic mapping can be found. This should take the same form as the `pages` property but should be a json string of the map. If a page appears in both the dynamic and static mapping, the dynamic one takes precedence.
For example, the following document has a field `foo` which stores the json string mapping indicating that its pdf has a w2 on page 1 and a w4 on page 5. 
```json 
{
  id: "1",
  path: "bar.pdf",
  foo: "{\"1\": \"w2\", \"5\": \"w4\"}"
}
```

## Addtional Properties 
The config must also contain the following two properties:
1. `path_field` (String) : The field which stores the path of the image for each document. If a document does not contain this field or the file does not exist, the document is skipped which the appropriate logging.
2. `lang` (String) : A 3 letter [LangCode](https://tesseract-ocr.github.io/tessdoc/Data-Files-in-different-versions.html) which tell Tesseract the language to be used for extraction. For whichever language that is specified, 
the appropriate [traineddata](https://github.com/tesseract-ocr/tessdata) file must be placed in a `TesseractOcr` directory located in the working directory from which lucille is run.  

