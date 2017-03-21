package model.flink.util;

public class Fraction {
	
	private double numerator;
	private double denominator;
	
	public Fraction() {
		this.numerator = 0.0;
		this.denominator = 0.0;
	}
	
	public Fraction(double numerator, double denominator) {
		this.numerator = numerator;
		this.denominator = denominator;
	}
	
	public void increaseNumerator() {
		numerator += 1.0; 
	}
	
	public void increaseDenominator() {
		denominator += 1.0; 
	}
	
	public double result() {
		return numerator / denominator;
	}
	
	public double getNumerator() {
		return numerator;
	}

	public void setNumerator(double numerator) {
		this.numerator = numerator;
	}

	public double getDenominator() {
		return denominator;
	}

	public void setDenominator(double denominator) {
		this.denominator = denominator;
	}
	
	
}
