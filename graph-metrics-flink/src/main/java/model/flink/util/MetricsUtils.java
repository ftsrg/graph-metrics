package model.flink.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import model.flink.Metrics;
import model.flink.metrics.helper.DimensionType;

public final class MetricsUtils {
	
	private final static String BENCHMARK_DIR = System.getProperty("user.dir") + "/src/main/benchmark/";
	
	public static void writeToFile(String path, double time) {
		File file = new File(BENCHMARK_DIR + path);
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(file);
			PrintWriter pw = new PrintWriter(fileWriter);
			pw.println(time);
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void startMeasure(Graph<IntValue, NullValue, String> graph, DimensionType labelParameter) {
		Metrics metrics = new Metrics(graph);
		
		
		
		long starttime = 0;
		long endtime = 0;
		long elapsed = 0;
		
		starttime = System.nanoTime();
		metrics.inDegree();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("in_degree", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.outDegree();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("out_degree", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.averageDegree();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("average_degree", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.localClusteringCoefficient();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("lcc", (double) elapsed / 1000000000.0);
		
		
		starttime = System.nanoTime();
		metrics.dimensionalDegree(null, Stream.of(DimensionType.HAS_POST).collect(Collectors.toList()));
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("dimensional_degree", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.nodeDimensionActivity(DimensionType.CONTAINER_OF);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("node_dimension_activity", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.nodeDimensionConnectivity(labelParameter);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("node_dimension_connectivity", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.nodeExclusiveDimensionConnectivity(DimensionType.CONTAINER_OF);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("nedc", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.edgeDimensionActivity(DimensionType.CONTAINER_OF);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("edge_dimension_activity", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.edgeDimensionConnectivity(DimensionType.CONTAINER_OF);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("edge_dimension_connectivity", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.nodeActivity(8);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("node_activity", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.multiplexParticipationCoefficient(1);
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("mpc", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.dc1();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("dc1", (double) elapsed / 1000000000.0);
		
		starttime = System.nanoTime();
		metrics.dc2();
		endtime = System.nanoTime();
		elapsed = endtime - starttime;
		writeToFile("dc2", (double) elapsed / 1000000000.0);
	}

	private MetricsUtils() {
	}
}
