package com.kmwllc.lucille.ocr.stage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Rectangle {
	public String label;
	public int x;
	public int y;
	public int width;
	public int height;

	@JsonCreator
	public Rectangle(
		@JsonProperty("label") String label,
		@JsonProperty("x") int x,
		@JsonProperty("y") int y,
		@JsonProperty("width") int width,
		@JsonProperty("height") int height) {
		this.label = label;
		this.x = x;
		this.y = y;
		this.width = width;
		this.height = height;
	}

	@Override
	public String toString() {
		return "Rectangle{" +
			"label='" + label + '\'' +
			", x=" + x +
			", y=" + y +
			", width=" + width +
			", height=" + height +
			'}';
	}
}
