package com.kmwllc.lucille.util;

// Requires ClassGraph (io.github.classgraph:classgraph:4.8.158) on the classpath
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;

/**
 * Utility for scanning the classpath and finding subclasses
 * of a given parent class within a specific package using ClassGraph.
 */
public class ClassUtils {

  /**
   * Logger for the ClassUtils.
   */
  public static final Logger log = LoggerFactory.getLogger(ClassUtils.class);

  /**
   * Data Transfer Object (DTO) for serializing Method information.
   */
  public static class MethodDescriptor {

    private final String declaringClass;
    private final String methodName;
    private final List<String> parameterTypes;
    private final String returnType;
    private final List<String> parameterNames;
    private String description; // Optional field for description

    /**
     * Constructs a MethodDescriptor from a java.lang.reflect.Method.
     *
     * @param method the Method to describe
     * @param parameterNames the names of the parameters
     */
    public MethodDescriptor(Method method, List<String> parameterNames) {
      this.declaringClass = method.getDeclaringClass().getName();
      this.methodName = method.getName();
      this.parameterTypes = Arrays.stream(method.getParameterTypes())
          .map(Class::getName)
          .collect(Collectors.toList());
      this.returnType = method.getReturnType().getName();
      this.parameterNames = parameterNames;
    }

    /**
     * Constructs a MethodDescriptor without requiring a Method instance.
     * Useful for doclets that don't want to load classes.
     *
     * @param methodName the name of the method
     * @param parameterNames the names of the parameters
     * @param parameterTypes the types of the parameters as strings
     */
    public MethodDescriptor(String methodName, List<String> parameterNames, List<String> parameterTypes) {
      this.declaringClass = null; // Will be populated when added to a ClassDescriptor
      this.methodName = methodName;
      this.parameterTypes = parameterTypes;
      this.returnType = null; // May be populated later
      this.parameterNames = parameterNames;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public String getDeclaringClass() {
      return declaringClass;
    }

    public String getMethodName() {
      return methodName;
    }

    public List<String> getParameterTypes() {
      return parameterTypes;
    }

    public String getReturnType() {
      return returnType;
    }

    public List<String> getParameterNames() {
      return parameterNames;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof MethodDescriptor))
        return false;
      MethodDescriptor that = (MethodDescriptor) o;
      return Objects.equals(declaringClass, that.declaringClass)
          && Objects.equals(methodName, that.methodName)
          && Objects.equals(parameterTypes, that.parameterTypes)
          && Objects.equals(returnType, that.returnType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(declaringClass, methodName, parameterTypes, returnType);
    }

    @Override
    public String toString() {
      return declaringClass + "#" + methodName
          + parameterTypes.toString()
          + " : " + returnType;
    }
  }

  /**
   * Data Transfer Object (DTO) for serializing Class information.
   */
  public static class ClassDescriptor {
    private final String className;
    private final String packageName;
    private final String superClassName;
    private final List<String> interfaceNames;
    private final boolean isAbstract;
    private final List<MethodDescriptor> methods;
    private String description; // Optional field for description
    private Map<String, String> fieldDocs; // Field name to documentation mapping
    private boolean isConfigClass; // Whether this is a configuration class
    private String configForClass; // The class this config is for (if applicable)

    /**
     * Constructs a ClassDescriptor from a java.lang.Class.
     *
     * @param clazz the Class to describe
     */
    public ClassDescriptor(Class<?> clazz) {
      this.className = clazz.getName();
      Package pkg = clazz.getPackage();
      this.packageName = (pkg != null) ? pkg.getName() : "";
      Class<?> superCls = clazz.getSuperclass();
      this.superClassName = (superCls != null) ? superCls.getName() : null;
      this.interfaceNames = Arrays.stream(clazz.getInterfaces())
          .map(Class::getName)
          .collect(Collectors.toList());
      this.isAbstract = Modifier.isAbstract(clazz.getModifiers());
      this.methods = Arrays.stream(clazz.getDeclaredMethods())
          .map(method -> new MethodDescriptor(method, null)) // Placeholder for parameter names
          .collect(Collectors.toList());
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
     * @param simpleClassName The simple name of the class
     * @param methods The list of methods for this class
     */
    public ClassDescriptor(String className, String simpleClassName, List<MethodDescriptor> methods) {
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

    /**
     * Mark this class as a configuration class.
     *
     * @param isConfigClass True if this is a configuration class
     */
    public void setIsConfigClass(boolean isConfigClass) {
      this.isConfigClass = isConfigClass;
    }

    /**
     * Check if this is a configuration class.
     *
     * @return True if this is a configuration class
     */
    public boolean isConfigClass() {
      return isConfigClass;
    }

    /**
     * Set the class this config is for.
     *
     * @param configForClass The fully qualified name of the class this config is for
     */
    public void setConfigForClass(String configForClass) {
      this.configForClass = configForClass;
    }

    /**
     * Get the class this config is for.
     *
     * @return The fully qualified name of the class this config is for
     */
    public String getConfigForClass() {
      return configForClass;
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

  private ClassUtils() {
    // Prevent instantiation
  }

  /**
   * Finds all concrete subclasses of the specified parent class within the given
   * package.
   *
   * @param parentClass the abstract/base class or interface
   * @param packageName the root package to scan (e.g.,
   *                    "com.kmwllc.lucille.stage")
   * @param <T>         the type of the parent class
   * @return a Set of classes extending or implementing the parentClass
   */
  @SuppressWarnings("unchecked")
  public static <T> Set<Class<? extends T>> findSubclasses(Class<T> parentClass, String packageName) {
    Set<Class<? extends T>> result = new HashSet<>();
    try (ScanResult scanResult = new ClassGraph()
        .enableAllInfo()
        .acceptPackages(packageName)
        .scan()) {

      for (ClassInfo classInfo : scanResult.getSubclasses(parentClass.getName())) {
        Class<?> cls = classInfo.loadClass();
        if (parentClass.isAssignableFrom(cls) && !Modifier.isAbstract(cls.getModifiers())) {
          result.add((Class<? extends T>) cls);
        }
      }
    }
    return result;
  }

  /**
   * Finds all concrete subclasses of the specified parent class within the given
   * package and returns a set of ClassDescriptors for each found class.
   *
   * @param parentClassName the fully qualified name of the abstract/base class or
   *                        interface
   * @param packageName     the root package to scan (e.g.,
   *                        "com.kmwllc.lucille.stage")
   * @return a Set of ClassDescriptor objects describing each subclass
   * @throws ClassNotFoundException if the parent class cannot be found
   */
  public static Set<ClassDescriptor> findSubclassDescriptors(String parentClassName, String packageName)
      throws ClassNotFoundException {
    log.info("Finding subclasses of {} in package {}", parentClassName, packageName);
    Set<ClassDescriptor> result = new HashSet<>();
    Class<?> parentClass = Class.forName(parentClassName);

    try (ScanResult scanResult = new ClassGraph()
        .enableAllInfo()
        .acceptPackages(packageName)
        .scan()) {

      // print out alll class names scanned
      for (ClassInfo classInfo : scanResult.getAllClasses()) {
        // log.info("Scanned class: {}", classInfo.getName());
        System.out.println("Scanned class: " + classInfo.getName());
      }
    }

    try (ScanResult scanResult = new ClassGraph()
        .enableAllInfo()
        .acceptPackages(packageName)
        .scan()) {

      for (ClassInfo classInfo : scanResult.getSubclasses(parentClassName)) {
        Class<?> cls = classInfo.loadClass();
        if (parentClass.isAssignableFrom(cls) && !Modifier.isAbstract(cls.getModifiers())) {
          result.add(new ClassDescriptor(cls));
        }
      }
    }
    return result;
  }

  /**
   * Finds all concrete classes implementing the specified interface within the given
   * package and returns a set of ClassDescriptors for each found class.
   *
   * @param interfaceName the fully qualified name of the interface
   * @param packageName     the root package to scan (e.g.,
   *                        "com.kmwllc.lucille.stage")
   * @return a Set of ClassDescriptor objects describing each class implementing the interface
   * @throws ClassNotFoundException if the interface cannot be found
   */
  public static Set<ClassDescriptor> findInterfaceDescriptors(String interfaceName, String packageName)
      throws ClassNotFoundException {
    log.info("Finding classes implementing interface {} in package {}", interfaceName, packageName);
    Set<ClassDescriptor> result = new HashSet<>();
    Class<?> interf = Class.forName(interfaceName);

    if (!interf.isInterface()) {
      throw new IllegalArgumentException(interfaceName + " is not an interface");
    }

    try (ScanResult scanResult = new ClassGraph()
        .enableAllInfo()
        .acceptPackages(packageName)
        .scan()) {

      for (ClassInfo classInfo : scanResult.getClassesImplementing(interfaceName)) {
        Class<?> cls = classInfo.loadClass();
        if (!Modifier.isAbstract(cls.getModifiers()) && !cls.isInterface()) {
          result.add(new ClassDescriptor(cls));
        }
      }
    }
    return result;
  }
}