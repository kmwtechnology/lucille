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
import javax.lang.model.element.VariableElement;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.Writer;
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
 * A modern JDK-9+ Doclet that generates a JSON file of ClassDescriptor and MethodDescriptor.
 * This doclet extracts documentation from source code structure rather than loading runtime classes.
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
        ClassDescriptor classDesc = new ClassDescriptor(className, simpleClassName, methodDescriptors);
        
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
        List<ClassDescriptor> descriptors = new ArrayList<>();

        for (Element e : env.getIncludedElements()) {
            if (!(e instanceof TypeElement)) continue;
            TypeElement topLevelType = (TypeElement) e;
            
            // Process the top-level class itself
            ClassDescriptor topLevelDesc = createClassDescriptorFromType(topLevelType, env);
            if (topLevelDesc != null) {
                descriptors.add(topLevelDesc);
            }
            
            // Process public static inner classes - especially looking for Config classes
            for (Element enclosedElement : topLevelType.getEnclosedElements()) {
                if (enclosedElement instanceof TypeElement) {
                    TypeElement innerType = (TypeElement) enclosedElement;
                    Set<Modifier> modifiers = innerType.getModifiers();
                    
                    // Check if it's a public static inner class/enum
                    if ((innerType.getKind() == ElementKind.CLASS || innerType.getKind() == ElementKind.ENUM) &&
                        modifiers.contains(Modifier.PUBLIC) && 
                        modifiers.contains(Modifier.STATIC)) {
                        
                        // Check if it follows the naming convention: OuterClassNameConfig
                        String outerName = topLevelType.getSimpleName().toString();
                        String innerName = innerType.getSimpleName().toString();
                        boolean isConfigClass = innerName.equals(outerName + "Config");
                        
                        ClassDescriptor innerDesc = createClassDescriptorFromType(innerType, env);
                        if (innerDesc != null) {
                            // Mark this as a configuration class if it follows the naming convention
                            if (isConfigClass) {
                                innerDesc.setIsConfigClass(true);
                                innerDesc.setConfigForClass(topLevelType.getQualifiedName().toString());
                            }
                            descriptors.add(innerDesc);
                        }
                    }
                }
            }
        }

        try {
            javax.tools.JavaFileManager fileManager = javax.tools.ToolProvider.getSystemJavaCompiler().getStandardFileManager(null, null, null);
            FileObject file = fileManager.getFileForOutput(StandardLocation.CLASS_OUTPUT, "", "javadocs.json", null);
            try (Writer w = file.openWriter()) {
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                gson.toJson(descriptors, new TypeToken<List<ClassDescriptor>>(){}.getType(), w);
            }
            return true;
        } catch (Exception e) {
            reporter.print(Diagnostic.Kind.ERROR, "Error writing javadocs.json: " + e.getMessage());
            return false;
        }
    }

    /**
     * Main method to run JsonDoclet as a standalone application.
     * Uses sensible defaults for source path, package, and output file.
     */
    public static void main(String[] args) throws Exception {
        // Default settings
        String sourcePath = "src/main/java";
        String subpackages = "com.kmwllc.lucille.connector";
        String outputFile = "target/javadocs.json";

        // Build javadoc tool args
        String[] javadocArgs = new String[] {
            "-doclet", JsonDoclet.class.getName(),
            "-sourcepath", sourcePath,
            "-subpackages", subpackages
        };

        // Use ToolProvider to invoke javadoc
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
