package model.flink;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;

public final class MetricsUtils {

	public static void startMeasure(Graph<IntValue, String, String> graph, DimensionType labelParameter) {
		Metrics metrics = new Metrics(graph);
		metrics.degrees();
		metrics.globalClusteringCoefficient();
		metrics.dimensionalDegree(null, Stream.of(DimensionType.OUTGOING, DimensionType.TARGET, DimensionType.INCOMING).collect(Collectors.toList()));
		metrics.nodeDimensionActivity(labelParameter);
		metrics.nodeDimensionConnectivity(labelParameter);
		metrics.nodeExclusiveDimensionConnectivity(labelParameter);
		metrics.edgeDimensionActivity(labelParameter);
		metrics.edgeDimensionConnectivity(labelParameter);
		metrics.nodeActivity(8);
		metrics.multiplexParticipationCoefficient(1);
		metrics.triangleListing();
	}

	private MetricsUtils() {
	}
}
