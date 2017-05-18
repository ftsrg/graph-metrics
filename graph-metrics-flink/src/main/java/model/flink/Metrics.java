package model.flink;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient.Result;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;

import model.flink.metrics.helper.DC1Metrics;
import model.flink.metrics.helper.DC2Metrics;
import model.flink.metrics.helper.DimensionType;
import model.flink.metrics.helper.DimensionalDegree;
import model.flink.metrics.helper.EdgeDimensionActivityFilter;
import model.flink.metrics.helper.NodeActivity;
import model.flink.metrics.helper.NodeDimensionActivity;
import model.flink.metrics.helper.NodeExclusiveDimensionActivity;

public class Metrics {

	final static Logger logger = Logger.getLogger(Metrics.class);
	private Graph<IntValue, NullValue, String> graph;
	private static final String RESOURCES_DIR = System.getProperty("user.dir") + "/src/main/resources/";
	public Metrics(Graph<IntValue, NullValue, String> graph) {
		this.graph = graph;
	}
	
	public void writeToFile(String path, DataSet<Tuple2<IntValue, LongValue>> dataSet) throws Exception {
		File file = new File(RESOURCES_DIR + path);
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);
		List<Tuple2<IntValue, LongValue>> dataList = dataSet.collect();
		for (Tuple2<IntValue, LongValue> result : dataList) {
			pw.println(result.f0.getValue() + " " + result.f1);
		}
		pw.close();
	}
	
	public void writeToIntValueIntegerTuple2ToFile(String path, DataSet<Tuple2<IntValue, Integer>> dataSet) throws Exception {
		File file = new File(RESOURCES_DIR + path);
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);
		List<Tuple2<IntValue, Integer>> dataList = dataSet.collect();
		for (Tuple2<IntValue, Integer> result : dataList) {
			pw.println(result.f0.getValue() + " " + result.f1);
		}
		pw.close();
	}
	
	public void writeResultToFile(String path, DataSet<Result<IntValue>> dataSet) throws Exception {
		File file = new File(RESOURCES_DIR + path);
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);
		List<Result<IntValue>> dataList = dataSet.collect();
		for (Result<IntValue> result : dataList) {
			pw.println(result.f0.getValue() + " " + result.f1);
		}
		pw.close();
	}
	
	public void writeNumberToFile(String path, Number number) throws Exception {
		File file = new File(RESOURCES_DIR + path);
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);
		pw.println(number);
		pw.close();
	}
	
	public void inDegree() {
		try {
			writeToFile("in_degree", graph.inDegrees());
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void outDegree() {
		try {
			writeToFile("out_degree", graph.outDegrees());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void averageDegree() {
		try {
			double averageDegree = (double) (graph.numberOfEdges() * 2) / graph.numberOfVertices();
			writeNumberToFile("average_degree", averageDegree);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void localClusteringCoefficient() {
		LocalClusteringCoefficient<IntValue, NullValue, String> algorithm = new LocalClusteringCoefficient<>();
		DataSet<Result<IntValue>> result;
		try {
			result = graph.run(algorithm);
			writeResultToFile("LCC", result);
		} catch (Exception exception) {
			logger.error(exception);
		}
	}

	public void dimensionalDegree(Integer id, List<DimensionType> dimensions) {
		DataSet<Tuple2<IntValue, Integer>> result = null;
		try {
			result = graph.groupReduceOnEdges(new DimensionalDegree(id, dimensions), EdgeDirection.ALL);
			writeToIntValueIntegerTuple2ToFile("dimensional_degree", result);
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

	public void nodeDimensionActivity(DimensionType dimension) {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeDimensionActivity(dimension), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> collect = dataSet.sum(1).collect();
			writeNumberToFile("node_dimension_activity", collect.get(0).f1);
		} catch (Exception exception) {
			logger.error(exception);
		}

	}

	public void nodeDimensionConnectivity(DimensionType dimension) {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeDimensionActivity(dimension), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> result = dataSet.sum(1).collect();
			System.out.println("NDC: " + ((double) result.get(0).f1) / dataSet.count());

		} catch (Exception exception) {
			logger.error(exception);
		}
	}

	public void nodeExclusiveDimensionConnectivity(DimensionType dimension) {
		DataSet<Tuple2<IntValue, Integer>> dataSet = graph.groupReduceOnEdges(new NodeExclusiveDimensionActivity(dimension), EdgeDirection.ALL);
		try {
			List<Tuple2<IntValue, Integer>> result = dataSet.sum(1).collect();
			System.out.println("NEDC: " + ((double) result.get(0).f1) / dataSet.count());

		} catch (Exception exception) {
			logger.error(exception);
		}
	}

	public void edgeDimensionActivity(DimensionType dimension) {
		Graph<IntValue, NullValue, String> edgeFilteredGraph = graph.filterOnEdges(new EdgeDimensionActivityFilter(dimension));
		try {
			System.out.println("EDA: " + edgeFilteredGraph.getEdges().count());
		} catch (Exception exception) {
			logger.error(exception);
		}
	}

	public void edgeDimensionConnectivity(DimensionType dimension) {
		Graph<IntValue, NullValue, String> edgeFilteredGraph = graph.filterOnEdges(new EdgeDimensionActivityFilter(dimension));
		try {
			System.out.println("EDC: " + ((double) edgeFilteredGraph.getEdges().count()) / graph.getEdges().count());
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
			double dimensionScale = ((double) dimensionalDegreeWithValue(id, Arrays.asList(new DimensionType[] { dimension }))) / dimensionalDegreeWithValue(id, Arrays.asList(DimensionType.values()));
			sum += dimensionScale * dimensionScale;
		}
		double mpc = (((double) dimensionLength) / (dimensionLength - 1)) * (1.0 - sum);
		System.out.println("MPC: " + mpc);
	}

	public void triangleListing() {
		TriangleListing<IntValue, NullValue, String> triangleListing = new TriangleListing<>();
		DataSet<Tuple3<IntValue, IntValue, IntValue>> result;
		
		try {
			result = graph.getUndirected().run(triangleListing);
			result.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void dc1() {
		DataSet<Tuple2<IntValue, Double>> result = graph.groupReduceOnEdges(new DC1Metrics(), EdgeDirection.ALL);
		try {
			result.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public void dc2() {
		DataSet<Tuple2<IntValue, Double>> result = graph.groupReduceOnEdges(new DC2Metrics(), EdgeDirection.ALL);
		try {
			result.print();
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
}
