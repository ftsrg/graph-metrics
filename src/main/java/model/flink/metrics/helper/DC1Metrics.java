package model.flink.metrics.helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import model.flink.util.EdgeUtils;
import model.flink.util.Fraction;

public class DC1Metrics implements EdgesFunctionWithVertexValue<IntValue, String, String, Tuple2<IntValue, Double>> {

	@Override
	public void iterateEdges(Vertex<IntValue, String> vertex, Iterable<Edge<IntValue, String>> edges, Collector<Tuple2<IntValue, Double>> out) throws Exception {
		ArrayList<Edge<IntValue, String>> edgeList = Lists.newArrayList(edges);
		Fraction fraction = new Fraction();
		for (int i = 0; i < edgeList.size(); i++) {
			for (int j = i + 1; j < edgeList.size(); j++) {
				Edge<IntValue, String> edge1 = edgeList.get(i);
				Edge<IntValue, String> edge2 = edgeList.get(j);
				// ha a két él azonos
				if (edge1.getValue().equals(edge2.getValue())) {
					List<String> triadCloserEdgeLabels = getTriadCloserEdgeLabels(edge1, edge2, vertex);
					if (triadCloserEdgeLabels.size() == 0) {
						fraction.increaseDenominator();
					} else {
						fraction.setNumerator(fraction.getNumerator() + triadCloserEdgeLabels.size());
						fraction.setDenominator(fraction.getDenominator() + triadCloserEdgeLabels.size());
					}
				}
			}
		}
		out.collect(new Tuple2<>(vertex.getId(), fraction.result()));
	}
	
	public List<String> getTriadCloserEdgeLabels(Edge<IntValue, String> edge1, Edge<IntValue, String> edge2, Vertex<IntValue, String> vertex) {
		// ki kell választani az elsõ él másik felét, nem tudjuk merre állnak az élek.
		IntValue edge1OtherVertexId = (edge1.getSource().getValue() == vertex.getId().getValue()) ? edge1.getTarget() : edge1.getSource();
		IntValue edge2OtherVertexId = (edge2.getSource().getValue() == vertex.getId().getValue()) ? edge2.getTarget() : edge2.getSource();
		List<String> labels = new ArrayList<>();
		List<Edge<IntValue, String>> edgeList = EdgeUtils.getEdgeList(edge1OtherVertexId);
		for (Edge<IntValue, String> edge : edgeList) {
			if (edge.getTarget().getValue() == edge2OtherVertexId.getValue() && !edge.getValue().equals(edge1.getValue())) {
				labels.add(edge.f2);
			}
		}
		return labels;
	}

}
