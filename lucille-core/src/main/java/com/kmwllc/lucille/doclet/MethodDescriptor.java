package com.kmwllc.lucille.doclet;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Data Transfer Object (DTO) for serializing Method information.
 */
public class MethodDescriptor {

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