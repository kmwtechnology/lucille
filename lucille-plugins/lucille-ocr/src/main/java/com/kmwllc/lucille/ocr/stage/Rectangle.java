package com.kmwllc.lucille.ocr.stage;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Rectangle implements Serializable {
	private String dest;
	private int x;
	private int y;
	private int width;
	private int height;



	public String getDest() {
    return dest;
  }



  public void setDest(String label) {
    this.dest = label;
  }



  public int getX() {
    return x;
  }



  public void setX(int x) {
    this.x = x;
  }



  public int getY() {
    return y;
  }



  public void setY(int y) {
    this.y = y;
  }



  public int getWidth() {
    return width;
  }



  public void setWidth(int width) {
    this.width = width;
  }



  public int getHeight() {
    return height;
  }



  public void setHeight(int height) {
    this.height = height;
  }



  @Override
	public String toString() {
		return "Rectangle{" +
			"label='" + dest + '\'' +
			", x=" + x +
			", y=" + y +
			", width=" + width +
			", height=" + height +
			'}';
	}
}
