package model.flink;

import org.apache.log4j.Logger;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;

public class Metrics {
	
	final static Logger logger = Logger.getLogger(Metrics.class);
	private Graph<IntValue, String, String> graph;
	
	public Metrics(Graph<IntValue, String, String> graph) {
		this.graph = graph;
	}
	
	public void dimensionalDegree() {
		DataSet<Tuple2<IntValue, Integer>> result = graph.groupReduceOnEdges(new DimensionalDegree(DimensionType.OUTGOING), EdgeDirection.ALL);
		try {
			result.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void nodeDimensionActivity() {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeDimensionActivity(DimensionType.OUTGOING), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> collect = dataSet.sum(1).collect();
			System.out.println("NDA: " + collect.get(0).f1);
		} catch (Exception exception) {
			logger.error(exception);
		}
		
		
	}
	
	public void nodeDimensionConnectivity() {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeDimensionActivity(DimensionType.OUTGOING), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> result = dataSet.sum(1).collect();
			System.out.println("NDC: " + ((double) result.get(0).f1) / dataSet.count());
			
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void nodeExclusiveDimensionConnectivity() {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeExclusiveDimensionActivity(DimensionType.OUTGOING), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> result = dataSet.sum(1).collect();
			System.out.println("NEDC: " + ((double) result.get(0).f1) / dataSet.count());
			
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
}
