package model.flink.metrics.helper;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * This class measures the dimensional degree of a vertex in specified dimensions.
 * 
 * @author Lehel
 *
 */
public final class DimensionalDegree implements EdgesFunctionWithVertexValue<IntValue, NullValue, String, Tuple2<IntValue, Integer>> {
	private static final long serialVersionUID = 1L;
	private IntValue id;
	private List<DimensionType> dimensions;

	/**
	 * Public constructor.
	 * 
	 * @param id
	 *            - the vertex to be measured
	 * @param dimensions
	 *            - list of dimensions
	 */
	public DimensionalDegree(Integer id, List<DimensionType> dimensions) {
		if (id != null) {
			this.id = new IntValue(id);
		}
		this.dimensions = dimensions;
	}

	/**
	 * Iterates through the vertices of the graph, collects information.
	 * 
	 * @param vertex
	 *            - the current vertex of the graph
	 * @param edges
	 *            - the edges of the current vertex
	 * @param out
	 *            - stores information about id's and their dimensional degrees.
	 */
	@Override
	public void iterateEdges(Vertex<IntValue, NullValue> vertex, Iterable<Edge<IntValue, String>> edges, Collector<Tuple2<IntValue, Integer>> out) throws Exception {
		int degree = 0;
		if (id != null) {
			if (id.getValue() == vertex.getId().getValue()) {
				for (Edge<IntValue, String> edge : edges) {
					DimensionType edgeDimension = DimensionType.getEnumByName(edge.getValue());
					if (dimensions.contains(edgeDimension)) {
						degree++;
					}
				}
				out.collect(new Tuple2<>(vertex.getId(), degree));
			}
		} else {
			for (Edge<IntValue, String> edge : edges) {
				DimensionType edgeDimension = DimensionType.getEnumByName(edge.getValue());
				if (dimensions.contains(edgeDimension)) {
					degree++;
				}
			}
			out.collect(new Tuple2<>(vertex.getId(), degree));
		}

	}
}
