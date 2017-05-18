package model.flink.metrics.helper;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public final class NodeActivity implements EdgesFunctionWithVertexValue<IntValue, NullValue, String, Tuple2<IntValue, Integer>> {
	private static final long serialVersionUID = 1L;
	private IntValue id;

	public NodeActivity(IntValue id) {
		this.id = id;
	}

	@Override
	public void iterateEdges(Vertex<IntValue, NullValue> vertex, Iterable<Edge<IntValue, String>> edges, Collector<Tuple2<IntValue, Integer>> out) throws Exception {
//		if (id.getValue() == vertex.getId().getValue()) {
			Set<String> distinctEdgeDimensions = new HashSet<>();
			for (Edge<IntValue, String> edge : edges) {
				if (!distinctEdgeDimensions.contains(edge.getValue())) {
					distinctEdgeDimensions.add((String) edge.getValue());
				}
			}
			out.collect(new Tuple2<>(vertex.getId(), distinctEdgeDimensions.size()));
//		}

	}

}
