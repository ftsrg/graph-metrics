package model.flink.metrics.helper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

public final class NodeDimensionActivity implements EdgesFunctionWithVertexValue<IntValue, String, String, Tuple2<IntValue, Integer>> {
	private static final long serialVersionUID = 1L;
	private DimensionType dimension;

	public NodeDimensionActivity(DimensionType dimension) {
		this.dimension = dimension;
	}

	@Override
	public void iterateEdges(Vertex<IntValue, String> vertex, Iterable<Edge<IntValue, String>> edges, Collector<Tuple2<IntValue, Integer>> out) throws Exception {
		boolean isActive = false;
		for (Edge<IntValue, String> edge : edges) {
			if (edge.getValue().equals(dimension.getLabel())) {
				isActive = true;
				break;
			}
		}
		out.collect(new Tuple2<>(vertex.getId(), isActive ? 1 : 0));
	}
}