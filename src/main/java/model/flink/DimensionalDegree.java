package model.flink;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

final class DimensionalDegree implements EdgesFunctionWithVertexValue<IntValue, String, String, Tuple2<IntValue, Integer>> {
	private static final long serialVersionUID = 1L;
	private IntValue id;
	private List<DimensionType> dimensions;
	
	public DimensionalDegree(Integer id, List<DimensionType> dimensions) {
		if (id != null) {
			this.id = new IntValue(id);
		}
		this.dimensions = dimensions;
	}
	
	@Override
	public void iterateEdges(Vertex<IntValue, String> vertex, Iterable<Edge<IntValue, String>> edges,
			Collector<Tuple2<IntValue, Integer>> out) throws Exception {
		int degree = 0;
		if (id != null) {
			if(id.getValue() == vertex.getId().getValue()) {
				for (Edge<IntValue, String> edge : edges) {
					DimensionType edgeDimension = DimensionType.valueOf(edge.getValue().toUpperCase());
					if(dimensions.contains(edgeDimension)) {
						degree++;
					}
				}
				out.collect(new Tuple2<>(vertex.getId(), degree));
			}
		} else {
			for (Edge<IntValue, String> edge : edges) {
				DimensionType edgeDimension = DimensionType.valueOf(edge.getValue().toUpperCase());
				if(dimensions.contains(edgeDimension)) {
					degree++;
				}
			}
			out.collect(new Tuple2<>(vertex.getId(), degree));
		}
		
		
	}
}
