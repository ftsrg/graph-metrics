package model.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;

public class Main {
	public static void main(String[] args) throws Exception {

		// BasicConfigurator.configure();
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<IntValue, String, String> graph = GraphUtils.createGraph(env);
		MetricsUtils.startMeasure(graph, DimensionType.OUTGOING);
	}

}
