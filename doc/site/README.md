# Hugo Setup (Documentation Site)

Lucille’s documentation site is hosted on GitHub Pages using Hugo (a static web page generator) and Docsy (a popular Hugo theme/template for documentation sites). 
Hugo & Docsy can be installed locally so that developers can build and view their documentation changes before merging a PR.

To install:

Install or update hugo to the latest version (works with v0.155.3 or higher). Can verify version using 
```console
$ hugo version
```

Run the following commands in your terminal:
```bash
npm install -D autoprefixer
npm install -D postcss-cli
npm install -D postcss
```

Navigate to lucille/doc/site directory in your terminal and update docsy by running (skip this step if docsy is already updated to v 0.14.0 )
```console
$ hugo mod get -u github.com/google/docsy@v0.14.0
```


Finally, run
```console
$ hugo server
```

If this is giving permission issues from the doc/site/public folder run 
```console
$ chmod -R 755 <lucille-directory>/doc/site/public/ 
```

to update permissions.