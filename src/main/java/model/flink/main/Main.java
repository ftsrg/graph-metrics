package model.flink.main;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;

import model.flink.metrics.helper.DimensionType;
import model.flink.util.EdgeUtils;
import model.flink.util.GraphUtils;
import model.flink.util.MetricsUtils;

public class Main {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<IntValue, String, String> graph = GraphUtils.createGraph(env);
		EdgeUtils.init(graph);
		MetricsUtils.startMeasure(graph, DimensionType.OUTGOING);
	}

}
