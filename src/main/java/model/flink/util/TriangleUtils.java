package model.flink.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.IntValue;

import model.flink.metrics.helper.DimensionType;

public class TriangleUtils {
	
	public static void countDC1(Tuple6<IntValue, IntValue, IntValue, String, String, String> triangle, Map<IntValue, Fraction> dc1Values)  {
		List<IntValue> vertices = new ArrayList<>();
		vertices.add(triangle.f0);
		vertices.add(triangle.f1);
		vertices.add(triangle.f2);
		String[] firstEdgeSplit = triangle.f3.split(" ");
		String[] secondEdgeSplit = triangle.f4.split(" ");
		String[] thirdEdgeSplit = triangle.f5.split(" ");
		List<String[]> edgeSplit = new ArrayList<>();
		edgeSplit.add(firstEdgeSplit);
		edgeSplit.add(secondEdgeSplit);
		edgeSplit.add(thirdEdgeSplit);
		for (IntValue vertex : vertices) {
			String[] labels = new String[2];
			String otherLabel = null;
			int labelCount = 0;
			for (String[] split : edgeSplit) {
				IntValue firstVertex = new IntValue(Integer.parseInt(split[0]));
				IntValue secondVertex = new IntValue(Integer.parseInt(split[1]));
				if (vertex.getValue() == firstVertex.getValue() || vertex.getValue() == secondVertex.getValue()) {
					labels[labelCount++] = split[2];
				} else {
					otherLabel = split[2];
				}
			}
			if (labels.length == 2 && labels[0].equals(labels[1]) && !labels[0].equals(otherLabel)) {
				if (dc1Values.containsKey(vertex)) {
					Fraction fraction = dc1Values.get(vertex);
					fraction.setNumerator(fraction.getNumerator() + 1.0);
					fraction.setDenominator(fraction.getDenominator() + 1.0);
					dc1Values.put(vertex, fraction);
				} else {
					dc1Values.put(vertex, new Fraction(1.0, 1.0));
				}
			} else {
				if (dc1Values.containsKey(vertex)) {
					Fraction fraction = dc1Values.get(vertex);
					fraction.setDenominator(fraction.getDenominator() + 1.0);
					dc1Values.put(vertex, fraction);
				} else {
					dc1Values.put(vertex, new Fraction(0.0, 1.0));
				}
			}
		}
	}
	
	public static void countDC2(Tuple6<IntValue, IntValue, IntValue, String, String, String> triangle, Map<IntValue, DC2Fraction> dc2Values) {
		List<IntValue> vertices = new ArrayList<>();
		vertices.add(triangle.f0);
		vertices.add(triangle.f1);
		vertices.add(triangle.f2);
		String[] firstEdgeSplit = triangle.f3.split(" ");
		String[] secondEdgeSplit = triangle.f4.split(" ");
		String[] thirdEdgeSplit = triangle.f5.split(" ");
		List<String[]> edgeSplit = new ArrayList<>();
		edgeSplit.add(firstEdgeSplit);
		edgeSplit.add(secondEdgeSplit);
		edgeSplit.add(thirdEdgeSplit);
		for (IntValue vertex : vertices) {
			String[] neighborEdges = new String[2];
			String otherEdge = null;
			int labelCount = 0;
			DC2Fraction dc2Fraction = null;
			if (dc2Values.containsKey(vertex)) {
				dc2Fraction = dc2Values.get(vertex);				
			} else {
				dc2Fraction = new DC2Fraction();
			}
			for (String[] split : edgeSplit) {
				IntValue firstVertex = new IntValue(Integer.parseInt(split[0]));
				IntValue secondVertex = new IntValue(Integer.parseInt(split[1]));
				if (vertex.getValue() == firstVertex.getValue() || vertex.getValue() == secondVertex.getValue()) {
					neighborEdges[labelCount++] = split[2];
				} else {
					otherEdge = split[2];
				}
			}
			if (!neighborEdges[0].equals(neighborEdges[1])) {
				for (DimensionType dimensionType : DimensionType.values()) {
					if (!dimensionType.getLabel().equals(neighborEdges[0]) && !dimensionType.getLabel().equals(neighborEdges[1])
							&& !dimensionType.getLabel().equals(otherEdge)) {
						dc2Fraction.addToDenominator(dimensionType);
					}
				}
				if (!otherEdge.equals(neighborEdges[0]) && !otherEdge.equals(neighborEdges[1])) {
					DimensionType otherEdgeDimension = DimensionType.valueOf(otherEdge.toUpperCase());
					dc2Fraction.addToNumerator(otherEdgeDimension);
					dc2Fraction.addToDenominator(otherEdgeDimension);
				}
			}
			dc2Values.put(vertex, dc2Fraction);
		}
	}
	
}
