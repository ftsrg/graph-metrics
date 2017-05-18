package model.flink.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import model.flink.Metrics;
import model.flink.metrics.helper.DimensionType;

public final class MetricsUtils {

	public static void startMeasure(Graph<IntValue, NullValue, String> graph, DimensionType labelParameter) {
		Metrics metrics = new Metrics(graph);
//		metrics.inDegree();
//		metrics.outDegree();
//		metrics.averageDegree();
//		metrics.localClusteringCoefficient();
//		metrics.dimensionalDegree(null, Stream.of(DimensionType.HAS_POST).collect(Collectors.toList()));
//		metrics.nodeDimensionActivity(DimensionType.CONTAINER_OF);
//		metrics.nodeDimensionConnectivity(labelParameter);
		metrics.nodeExclusiveDimensionConnectivity(DimensionType.CONTAINER_OF);
//		metrics.edgeDimensionActivity(DimensionType.CONTAINER_OF);
//		metrics.edgeDimensionConnectivity(labelParameter);
//		metrics.nodeActivity(8);
//		metrics.multiplexParticipationCoefficient(1);
//		metrics.triangleListing();
//		metrics.dc1();
//		metrics.dc2();
	}

	private MetricsUtils() {
	}
}
