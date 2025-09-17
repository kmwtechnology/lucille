package com.kmwllc.lucille.doclet;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Data Transfer Object (DTO) for serializing Class information.
 */
public class ClassDescriptor {
  private final String className;
  private final String packageName;
  private final String superClassName;
  private final List<String> interfaceNames;
  private final boolean isAbstract;
  private final List<MethodDescriptor> methods;
  private String description; // Optional field for description
  private Map<String, String> fieldDocs; // Field name to documentation mapping

  /**
   * Constructs a ClassDescriptor from a java.lang.Class.
   *
   * @param clazz the Class to describe
   */
  public ClassDescriptor(Class<?> clazz) {
    this(clazz, Arrays.stream(clazz.getDeclaredMethods())
        .map(method -> new MethodDescriptor(method, null))
        .collect(Collectors.toList()));
  }

  /**
   * Constructs a ClassDescriptor from a java.lang.Class and a list of MethodDescriptors.
   *
   * @param clazz the Class to describe
   * @param methods the list of MethodDescriptors
   */
  public ClassDescriptor(Class<?> clazz, List<MethodDescriptor> methods) {
    this.className = clazz.getName();
    Package pkg = clazz.getPackage();
    this.packageName = (pkg != null) ? pkg.getName() : "";
    Class<?> superCls = clazz.getSuperclass();
    this.superClassName = (superCls != null) ? superCls.getName() : null;
    this.interfaceNames = Arrays.stream(clazz.getInterfaces())
        .map(Class::getName)
        .collect(Collectors.toList());
    this.isAbstract = Modifier.isAbstract(clazz.getModifiers());
    this.methods = methods;
  }

  /**
   * Constructs a ClassDescriptor without loading the class, using string information.
   *
   * @param className The fully qualified name of the class
   * @param methods The list of methods for this class
   */
  public ClassDescriptor(String className, List<MethodDescriptor> methods) {
    this.className = className;
    // Extract package from className
    this.packageName = className.contains(".") ?
        className.substring(0, className.lastIndexOf('.')) : "";
    this.superClassName = null; // May be populated later
    this.interfaceNames = new ArrayList<>(); // May be populated later
    this.isAbstract = false; // Default assumption, may be changed later
    this.methods = methods;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public String getClassName() {
    return className;
  }

  public String getPackageName() {
    return packageName;
  }

  public String getSuperClassName() {
    return superClassName;
  }

  public List<String> getInterfaceNames() {
    return interfaceNames;
  }

  public boolean isAbstract() {
    return isAbstract;
  }

  public List<MethodDescriptor> getMethods() {
    return methods;
  }

  /**
   * Sets documentation for fields in this class.
   *
   * @param fieldDocs Map from field names to their documentation
   */
  public void setFieldDocs(Map<String, String> fieldDocs) {
    this.fieldDocs = fieldDocs;
  }

  /**
   * Get the field documentation map.
   *
   * @return Map from field names to their documentation
   */
  public Map<String, String> getFieldDocs() {
    return fieldDocs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ClassDescriptor))
      return false;
    ClassDescriptor that = (ClassDescriptor) o;
    return isAbstract == that.isAbstract &&
        Objects.equals(className, that.className) &&
        Objects.equals(packageName, that.packageName) &&
        Objects.equals(superClassName, that.superClassName) &&
        Objects.equals(interfaceNames, that.interfaceNames) &&
        Objects.equals(methods, that.methods);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, packageName, superClassName, interfaceNames, isAbstract, methods);
  }

  @Override
  public String toString() {
    return "ClassDescriptor{" +
        "className='" + className + '\'' +
        ", packageName='" + packageName + '\'' +
        ", superClassName='" + superClassName + '\'' +
        ", interfaces=" + interfaceNames +
        ", isAbstract=" + isAbstract +
        ", methods=" + methods +
        '}';
  }
}