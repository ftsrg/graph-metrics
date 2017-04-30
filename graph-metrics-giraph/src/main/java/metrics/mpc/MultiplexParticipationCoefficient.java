package metrics.mpc;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

import util.DimensionType;

public class MultiplexParticipationCoefficient {

	public static class SendEdgeValues extends BasicComputation<LongWritable, Text, Text, Text> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<Text> messages) throws IOException {
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				sendMessage(edge.getTargetVertexId(), edge.getValue());
			}
		}
	}

	public static class MultiplexParticipationCoefficientComputation
			extends BasicComputation<LongWritable, Text, Text, Text> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<Text> messages) throws IOException {
			double allActiveDimensions = (double) (vertex.getNumEdges() + Iterables.size(messages));
			double dimensionSquareSum = 0;
			Map<DimensionType, Long> dimensionsMap = new HashMap<>();

			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				DimensionType edgeDimension = DimensionType.valueOf(edge.getValue().toString().toUpperCase());
				if (!dimensionsMap.containsKey(edgeDimension)) {
					dimensionsMap.put(edgeDimension, Long.valueOf(1));
				} else {
					dimensionsMap.put(edgeDimension, dimensionsMap.get(edgeDimension) + 1);
				}
			}

			for (Text message : messages) {
				DimensionType messageDimension = DimensionType.valueOf(message.toString().toUpperCase());
				if (!dimensionsMap.containsKey(messageDimension)) {
					dimensionsMap.put(messageDimension, Long.valueOf(1));
				} else {
					dimensionsMap.put(messageDimension, dimensionsMap.get(messageDimension) + 1);
				}
			}
			for (Entry<DimensionType, Long> entry : dimensionsMap.entrySet()) {
				dimensionSquareSum += ((double) entry.getValue() / allActiveDimensions)
						* ((double) entry.getValue() / allActiveDimensions);
			}
			double mpc = (double) (DimensionType.values().length) / (DimensionType.values().length - 1)
					* (1 - dimensionSquareSum);
			NumberFormat formatter = new DecimalFormat("#0.00");
			vertex.setValue(new Text(formatter.format(mpc)));
			vertex.voteToHalt();
		}
	}

	public static class MasterCompute extends DefaultMasterCompute {

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(SendEdgeValues.class);
			} else if (superstep == 1) {
				setComputation(MultiplexParticipationCoefficientComputation.class);
			}
		}
	}
}
