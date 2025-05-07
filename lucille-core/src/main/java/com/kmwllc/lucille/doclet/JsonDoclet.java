// src/main/java/com/kmwllc/lucille/doclet/JsonDoclet.java
package com.kmwllc.lucille.doclet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.kmwllc.lucille.util.ClassUtils;
import com.kmwllc.lucille.util.ClassUtils.ClassDescriptor;
import com.kmwllc.lucille.util.ClassUtils.MethodDescriptor;

import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.stream.Collectors;

import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

/**
 * A modern JDK-9+ Doclet that generates a JSON file of ClassDescriptor and MethodDescriptor
 */
public class JsonDoclet implements Doclet {
    private Reporter reporter;

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
        return Set.of();
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    @Override
    public boolean run(DocletEnvironment env) {
        List<ClassDescriptor> descriptors = new ArrayList<>();

        for (Element e : env.getIncludedElements()) {
            if (!(e instanceof TypeElement)) continue;
            TypeElement type = (TypeElement) e;
            if (type.getKind() != ElementKind.CLASS && type.getKind() != ElementKind.ENUM) continue;

            try {
                Class<?> runtimeClass = Class.forName(type.getQualifiedName().toString());
                // Build method descriptors with parameter names
                List<MethodDescriptor> methodDescriptors = new ArrayList<>();
                for (Element member : type.getEnclosedElements()) {
                    if (member instanceof ExecutableElement) {
                        ExecutableElement exec = (ExecutableElement) member;
                        if (exec.getKind() != ElementKind.METHOD) continue;
                        List<String> paramTypes = exec.getParameters().stream()
                                .map(p -> p.asType().toString())
                                .collect(Collectors.toList());
                        List<String> paramNames = exec.getParameters().stream()
                                .map(p -> p.getSimpleName().toString())
                                .collect(Collectors.toList());
                        Method method = null;
                        try {
                            method = runtimeClass.getDeclaredMethod(exec.getSimpleName().toString(),
                                    exec.getParameters().stream().map(p -> {
                                        try {
                                            return Class.forName(p.asType().toString());
                                        } catch (Exception ex) {
                                            return Object.class;
                                        }
                                    }).toArray(Class[]::new));
                        } catch (Exception ex) {
                            continue;
                        }
                        MethodDescriptor md = new MethodDescriptor(method, paramNames);
                        md.setDescription(env.getElementUtils().getDocComment(exec));
                        methodDescriptors.add(md);
                    }
                }
                // Use a constructor or reflection to set the methods field directly
                ClassDescriptor cd = new ClassDescriptor(runtimeClass, methodDescriptors);
                cd.setDescription(env.getElementUtils().getDocComment(type));
                descriptors.add(cd);
            } catch (Exception ex) {
                reporter.print(Diagnostic.Kind.WARNING, "Failed to process "
                    + e + ": " + ex.getMessage());
            }
        }

        try {
            javax.tools.JavaFileManager fileManager = javax.tools.ToolProvider.getSystemJavaCompiler().getStandardFileManager(null, null, null);
            FileObject file = fileManager.getFileForOutput(StandardLocation.CLASS_OUTPUT, "", "javadocs.json", null);
            try (Writer w = file.openWriter()) {
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                gson.toJson(descriptors, new TypeToken<List<ClassDescriptor>>(){}.getType(), w);
            }
        } catch (Exception e) {
            reporter.print(Diagnostic.Kind.ERROR, "Error writing javadocs.json: " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Main method to run JsonDoclet as a standalone application.
     * Uses sensible defaults for source path, package, and output file.
     */
    public static void main(String[] args) throws Exception {
        // Defaults
        String sourcePath = "src/main/java";
        String subpackages = "com.kmwllc.lucille.stage";
        String outputFile = "target/javadocs.json";

        // Build javadoc tool args
        String[] javadocArgs = new String[] {
            "-doclet", JsonDoclet.class.getName(),
            "-sourcepath", sourcePath,
            "-subpackages", subpackages
        };

        // Use ToolProvider to invoke javadoc (Java 9+)
        javax.tools.Tool javadocTool = javax.tools.ToolProvider.getSystemDocumentationTool();
        if (javadocTool == null) {
            System.err.println("Javadoc tool not found. Make sure you are using a JDK, not a JRE.");
            System.exit(1);
        }
        int result = javadocTool.run(null, System.out, System.err, javadocArgs);
        if (result == 0) {
            System.out.println("javadocs.json generated at: " + outputFile);
        } else {
            System.err.println("Javadoc execution failed with exit code: " + result);
            System.exit(result);
        }
    }
}

// -----------------------------------------------------------------------------
// Maven POM snippet for maven-javadoc-plugin (in <build><plugins>)
// -----------------------------------------------------------------------------
/**
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-javadoc-plugin</artifactId>
  <version>3.4.0</version>
  <configuration>
    <doclet>com.kmwllc.lucille.doclet.JsonDoclet</doclet>
    <docletArtifact>
      <groupId>com.kmwllc.lucille</groupId>
      <artifactId>lucille-core</artifactId>
      <version>${project.version}</version>
    </docletArtifact>
    <additionalJOptions>
      <additionalJOption>-Xdoclint:none</additionalJOption>
    </additionalJOptions>
  </configuration>
</plugin>
*/
