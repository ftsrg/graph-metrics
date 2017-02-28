package model.flink;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.log4j.Logger;
import java.lang.Exception;

public class Metrics {
	
	final static Logger logger = Logger.getLogger(Metrics.class);
	private Graph<IntValue, String, String> graph;
	
	public Metrics(Graph<IntValue, String, String> graph) {
		this.graph = graph;
	}
	
	public void dimensionalDegree(Integer id, List<DimensionType> dimensions) {
		DataSet<Tuple2<IntValue, Integer>> result = null;
		try {
			result = graph.groupReduceOnEdges(new DimensionalDegree(id, dimensions), EdgeDirection.ALL);
			result.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public Integer dimensionalDegreeWithValue(Integer id, List<DimensionType> dimensions) {
		if (id == null) {
			throw new IllegalArgumentException("dimensionalDegreeWithValue needs specific id as argument.");
		}
		DataSet<Tuple2<IntValue, Integer>> result = null;
		Integer degree = null;
		try {
			result = graph.groupReduceOnEdges(new DimensionalDegree(id, dimensions), EdgeDirection.ALL);
			degree = result.collect().get(0).f1;
		} catch (Exception exception) {
			logger.error(exception);
		}
		return degree;
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
	
	public void edgeDimensionActivity() {
		Graph<IntValue, String, String> edgeFilteredGraph = graph.filterOnEdges(new EdgeDimensionActivityFilter(DimensionType.OUTGOING));
		try {
			System.out.println("EDA: " + edgeFilteredGraph.getEdges().count());
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void edgeDimensionConnectivity() {
		Graph<IntValue, String, String> edgeFilteredGraph = graph.filterOnEdges(new EdgeDimensionActivityFilter(DimensionType.OUTGOING));
		try {
			System.out.println("EDA: " + ((double) edgeFilteredGraph.getEdges().count()) / graph.getEdges().count());
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void nodeActivity(Integer id) {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeActivity(new IntValue(id)), EdgeDirection.ALL);
		try {
			dataSet.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void multiplexParticipationCoefficient(Integer id) {
		int dimensionLength = DimensionType.values().length;
		double sum = 0.0;
		for (DimensionType dimension : DimensionType.values()) {
			double dimensionScale = ((double) dimensionalDegreeWithValue(id, Arrays.asList(new DimensionType[]{dimension}))) / 
					dimensionalDegreeWithValue(id, Arrays.asList(DimensionType.values()));
			sum += dimensionScale * dimensionScale;
		}
		double mpc = (((double) dimensionLength) / (dimensionLength - 1)) * (1.0 - sum);
		System.out.println("MPC: " + mpc);
	}
	
}
