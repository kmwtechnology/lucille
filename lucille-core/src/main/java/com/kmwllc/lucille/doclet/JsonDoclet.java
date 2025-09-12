package com.kmwllc.lucille.doclet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import java.io.Writer;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.stream.Collectors;

import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

/**
 * A modern JDK-9+ Doclet that generates a JSON file of ClassDescriptor and MethodDescriptor.
 * This doclet extracts documentation from source code structure rather than loading runtime classes.
 */
public class JsonDoclet implements Doclet {

  private Reporter reporter;
  private String outputFile = "javadocs.json"; // Default output file
  private String outputPath = "target"; // Default output directory
  private boolean helpWanted = false;

  @Override
  public void init(java.util.Locale locale, jdk.javadoc.doclet.Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public String getName() {
    return "JsonDoclet";
  }

  @Override
  public Set<? extends Option> getSupportedOptions() {
    return Set.of(
      // Option for output file name
      new Option() {
        @Override
        public int getArgumentCount() {
          return 1;
        }

        @Override
        public String getDescription() {
          return "Output file name (default: javadocs.json)";
        }

        @Override
        public Kind getKind() {
          return Kind.STANDARD;
        }

        @Override
        public List<String> getNames() {
          return List.of("-o", "--output");
        }

        @Override
        public String getParameters() {
          return "FILE";
        }

        @Override
        public boolean process(String option, List<String> arguments) {
          outputFile = arguments.get(0);
          return true;
        }
      },
      // Option for output directory
      new Option() {
        @Override
        public int getArgumentCount() {
          return 1;
        }

        @Override
        public String getDescription() {
          return "Output directory (default: target)";
        }

        @Override
        public Kind getKind() {
          return Kind.STANDARD;
        }

        @Override
        public List<String> getNames() {
          return List.of("-d", "--directory");
        }

        @Override
        public String getParameters() {
          return "DIR";
        }

        @Override
        public boolean process(String option, List<String> arguments) {
          outputPath = arguments.get(0);
          return true;
        }
      },
      // Help option
      new Option() {
        @Override
        public int getArgumentCount() {
          return 0;
        }

        @Override
        public String getDescription() {
          return "Display help information";
        }

        @Override
        public Kind getKind() {
          return Kind.STANDARD;
        }

        @Override
        public List<String> getNames() {
          return List.of("-h", "--help");
        }

        @Override
        public String getParameters() {
          return "";
        }

        @Override
        public boolean process(String option, List<String> arguments) {
          helpWanted = true;
          printHelp();
          return true;
        }
      }
    );
  }

  private static void printHelp() {
    System.out.println("JsonDoclet: Generates JSON documentation for Java classes");
    System.out.println("Usage: javadoc -doclet com.kmwllc.lucille.doclet.JsonDoclet [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  -o, --output FILE      Output file name (default: javadocs.json)");
    System.out.println("  -d, --directory DIR    Output directory (default: target)");
    System.out.println("  -h, --help             Display this help message");
    System.out.println();
    System.out.println("Standard javadoc options:");
    System.out.println("  -sourcepath PATH[,PATH...]  Source path(s) for Java files, comma-delimited");
    System.out.println("  -subpackages PKG[,PKG...]   Process the specified package(s), comma-delimited");
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latest();
  }

  /**
   * Creates a ClassDescriptor for a TypeElement without loading the class.
   *
   * @param type The TypeElement to process
   * @param env The DocletEnvironment
   * @return A ClassDescriptor for the type
   */
  private ClassDescriptor createClassDescriptorFromType(TypeElement type, DocletEnvironment env) {
    if (type.getKind() != ElementKind.CLASS && type.getKind() != ElementKind.ENUM) {
      return null;
    }

    String className = type.getQualifiedName().toString();
    String simpleClassName = type.getSimpleName().toString();

    List<MethodDescriptor> methodDescriptors = new ArrayList<>();

    // Process methods
    for (Element member : type.getEnclosedElements()) {
      if (member instanceof ExecutableElement && member.getKind() == ElementKind.METHOD) {
        ExecutableElement method = (ExecutableElement) member;

        // Only process public methods
        if (!method.getModifiers().contains(Modifier.PUBLIC)) {
          continue;
        }

        String methodName = method.getSimpleName().toString();

        // Get parameter names for documentation
        List<String> paramNames = method.getParameters().stream()
            .map(p -> p.getSimpleName().toString())
            .collect(Collectors.toList());

        // Get parameter types
        List<String> paramTypes = method.getParameters().stream()
            .map(p -> p.asType().toString())
            .collect(Collectors.toList());

        // Create a MethodDescriptor without using reflection
        MethodDescriptor methodDesc = new MethodDescriptor(methodName, paramNames, paramTypes);

        // Add doc comment if available
        String docComment = env.getElementUtils().getDocComment(method);
        if (docComment != null) {
          methodDesc.setDescription(docComment.trim());
        }

        methodDescriptors.add(methodDesc);
      }
    }

    // Create ClassDescriptor without loading the class
    ClassDescriptor classDesc = new ClassDescriptor(className, methodDescriptors);

    // Add class-level documentation
    String classDocComment = env.getElementUtils().getDocComment(type);
    if (classDocComment != null) {
      classDesc.setDescription(classDocComment.trim());
    }

    // Process field documentation for potential configuration properties
    processFieldDocumentation(type, env, classDesc);

    return classDesc;
  }

  /**
   * Process field documentation for a type element and add it to the class descriptor.
   * This is especially useful for configuration fields.
   *
   * @param type The type element
   * @param env The doclet environment
   * @param classDesc The class descriptor to augment
   */
  private void processFieldDocumentation(TypeElement type, DocletEnvironment env, ClassDescriptor classDesc) {
    Map<String, String> fieldDocs = new HashMap<>();

    for (Element member : type.getEnclosedElements()) {
      if (member.getKind() == ElementKind.FIELD) {
        String fieldName = member.getSimpleName().toString();
        String docComment = env.getElementUtils().getDocComment(member);

        if (docComment != null && !docComment.trim().isEmpty()) {
          fieldDocs.put(fieldName, docComment.trim());
        }
      }
    }

    if (!fieldDocs.isEmpty()) {
      classDesc.setFieldDocs(fieldDocs);
    }
  }

  @Override
  public boolean run(DocletEnvironment env) {
    if (helpWanted) {
      return false; // Exit cleanly after showing help
    }

    List<ClassDescriptor> descriptors = new ArrayList<>();

    for (Element e : env.getIncludedElements()) {
      if (!(e instanceof TypeElement)) {
        continue;
      }
      TypeElement topLevelType = (TypeElement) e;

      // Process the top-level class itself
      ClassDescriptor topLevelDesc = createClassDescriptorFromType(topLevelType, env);
      if (topLevelDesc != null) {
        descriptors.add(topLevelDesc);
      }
    }

    try {
      // Ensure output directory exists
      File outputDir = new File(outputPath);
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }

      File outputJsonFile = new File(outputDir, outputFile);
      reporter.print(Diagnostic.Kind.NOTE, "Writing JSON documentation to " + outputJsonFile.getAbsolutePath());

      // Use standard Java file writing instead of JavaFileManager
      try (Writer writer = new java.io.FileWriter(outputJsonFile)) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        gson.toJson(descriptors, writer);
      }

      return true;
    } catch (Exception e) {
      reporter.print(Diagnostic.Kind.ERROR, "Error writing javadocs: " + e.getMessage());
      return false;
    }
  }

  /**
   * Main method to run JsonDoclet as a standalone application.
   * Uses command-line arguments to configure the doclet or defaults to sensible defaults.
   */
  public static void main(String[] args) throws Exception {
    // Default settings
    String sourcePath = "src/main/java";
    String subpackages = "com.kmwllc.lucille.stage";
    String outputFile = "javadocs.json";
    String outputDir = "target";

    // Parse arguments for help flag
    if (Arrays.asList(args).contains("-h") || Arrays.asList(args).contains("--help")) {
      printHelp();
    }

    // Process command line arguments
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("-sp") || args[i].equals("--sourcepath")) {
        sourcePath = args[i + 1];
        i++; // Skip the next argument since we've processed it
      } else if (args[i].equals("-sb") || args[i].equals("--subpackages")) {
        subpackages = args[i + 1];
        i++; // Skip the next argument
      } else if (args[i].equals("-o") || args[i].equals("--output")) {
        outputFile = args[i + 1];
        i++; // Skip the next argument
      } else if (args[i].equals("-d") || args[i].equals("--directory")) {
        outputDir = args[i + 1];
        i++; // Skip the next argument
      }
    }

    // Build javadoc tool args, handling multiple sourcepaths and subpackages
    List<String> javadocArgs = new ArrayList<>();
    javadocArgs.add("-doclet");
    javadocArgs.add(JsonDoclet.class.getName());

    // Handle multiple sourcepaths (comma-separated)
    if (sourcePath.contains(",")) {
      // For sourcepath with multiple paths, javadoc requires either:
      // 1. Multiple -sourcepath arguments (one for each path)
      // 2. A single -sourcepath with OS-specific path separator
      String pathSeparator = System.getProperty("path.separator"); // ":" on Unix/Linux/Mac, ";" on Windows
      javadocArgs.add("-sourcepath");
      javadocArgs.add(sourcePath.replace(",", pathSeparator));
    } else {
      javadocArgs.add("-sourcepath");
      javadocArgs.add(sourcePath);
    }

    // Handle multiple subpackages (comma-separated)
    if (subpackages.contains(",")) {
      // For subpackages, we need to add a separate -subpackages argument for each package
      for (String pkg : subpackages.split(",")) {
        javadocArgs.add("-subpackages");
        javadocArgs.add(pkg.trim());
      }
    } else {
      javadocArgs.add("-subpackages");
      javadocArgs.add(subpackages);
    }

    javadocArgs.add("-o");
    javadocArgs.add(outputFile);
    javadocArgs.add("-d");
    javadocArgs.add(outputDir);

    // Use ToolProvider to invoke javadoc
    javax.tools.Tool javadocTool = javax.tools.ToolProvider.getSystemDocumentationTool();
    if (javadocTool == null) {
      System.err.println("Javadoc tool not found. Make sure you are using a JDK, not a JRE.");
      System.exit(1);
    }

    System.out.println("Running JsonDoclet with args: " + String.join(" ", javadocArgs));
    int result = javadocTool.run(null, System.out, System.err,
        javadocArgs.toArray(new String[0]));

    if (result == 0) {
      System.out.println("JSON documentation generated successfully.");
    } else {
      System.err.println("Javadoc execution failed with exit code: " + result);
      System.exit(result);
    }
  }
}