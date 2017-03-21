package model.flink.metrics.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.log4j.Logger;

import model.flink.Metrics;
import model.flink.util.EdgeUtils;
import model.flink.util.Fraction;

public class DC1 {

	private final static Logger logger = Logger.getLogger(Metrics.class);
	private Map<IntValue, Fraction> dc1Map;
	private Graph<IntValue, String, String> graph;

	public DC1(Graph<IntValue, String, String> graph) {
		this.graph = graph;
		dc1Map = new HashMap<>();
	}

	public void countDC1() {
		List<IntValue> vertexIds;
		try {
			vertexIds = graph.getVertexIds().collect();
			for (IntValue vertex : vertexIds) {
				List<Edge<IntValue, String>> edges = EdgeUtils.getEdgeList(vertex);
				Fraction fraction = dc1Map.getOrDefault(vertex, new Fraction());
				for (int i = 0; i < edges.size(); i++) {
					for (int j = i + 1; j < edges.size(); j++) {
						Edge<IntValue, String> edge1 = edges.get(i);
						Edge<IntValue, String> edge2 = edges.get(j);
//						fraction.increaseDenominator();
						if (hasSameNeighbors(edge1, edge2)) {
							List<String> otherEdgeLabels = getOtherEdgeLabel(edge1, edge2);
							if (otherEdgeLabels.isEmpty()) {
								fraction.setDenominator(fraction.getDenominator() + 1.0);
							} else {
								fraction.setNumerator(fraction.getNumerator() + (double) otherEdgeLabels.size());
								fraction.setDenominator(fraction.getDenominator() + (double) otherEdgeLabels.size());
							}

						} else {

						}
						dc1Map.put(vertex, fraction);
					}
				}
			}
			print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}

	public boolean hasSameNeighbors(Edge<IntValue, String> edge1, Edge<IntValue, String> edge2) {
		if (edge1.getValue().equals(edge2.getValue())) {
			return true;
		}
		return false;
	}

	public List<String> getOtherEdgeLabel(Edge<IntValue, String> edge1, Edge<IntValue, String> edge2) {
		List<String> labels = new ArrayList<>();
		List<Edge<IntValue, String>> edgeList = EdgeUtils.getEdgeList(edge1.getTarget());
		for (Edge<IntValue, String> edge : edgeList) {
			if (edge.getSource().getValue() == edge2.getTarget().getValue() || edge.getTarget().getValue() == edge2.getTarget().getValue()) {
				labels.add(edge.f2);
			}
		}
		return labels;
	}

	public void print() {
		for (Entry<IntValue, Fraction> entry : dc1Map.entrySet()) {
			System.out.println(entry.getKey().getValue() + ": " + entry.getValue().result());
		}
	}
}
