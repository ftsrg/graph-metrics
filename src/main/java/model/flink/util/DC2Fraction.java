package model.flink.util;

import java.util.HashSet;
import java.util.Set;

import model.flink.metrics.helper.DimensionType;

public class DC2Fraction {

	private Set<DimensionType> numerator;
	private Set<DimensionType> denominator;
	
	public DC2Fraction() {
		numerator = new HashSet<>();
		denominator = new HashSet<>();
	}
	
	public double result() {
		if (denominator.size() != 0) {
			return ((double) numerator.size()) / denominator.size();
		}
		return 0.0;
	}
	
	public void addToNumerator(DimensionType dimensionType) {
		numerator.add(dimensionType);
	}
	
	public void addToDenominator(DimensionType dimensionType) {
		denominator.add(dimensionType);
	}
	
	public Set<DimensionType> getNumerator() {
		return numerator;
	}

	public Set<DimensionType> getDenominator() {
		return denominator;
	}
	
}
