package model.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.IntValue;

public class EdgeDimensionActivityFilter implements FilterFunction<Edge<IntValue, String>> {
	private static final long serialVersionUID = 1L;
	private DimensionType dimension;
	
	public EdgeDimensionActivityFilter(DimensionType dimension) {
		this.dimension = dimension;
	}

	@Override
	public boolean filter(Edge<IntValue, String> value) throws Exception {
		return value.getValue().equals(dimension.getLabel());
	}
	
	
}
