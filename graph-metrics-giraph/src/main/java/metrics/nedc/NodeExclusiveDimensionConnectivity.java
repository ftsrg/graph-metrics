package metrics.nedc;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import util.DimensionType;

public class NodeExclusiveDimensionConnectivity {

	public static class SendOutEdges extends BasicComputation<LongWritable, Text, Text, NEDCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<NEDCMessageWrapper> messages)
				throws IOException {
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				NEDCMessageWrapper message = new NEDCMessageWrapper();
				message.setSourceId(vertex.getId());
				message.setMessage(new Text());
				message.setEdgeValue(edge.getValue());
				sendMessage(edge.getTargetVertexId(), message);
			}

		}

	}

	public static class NEDCComputation extends BasicComputation<LongWritable, Text, Text, NEDCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<NEDCMessageWrapper> messages)
				throws IOException {
			DimensionType dimension = DimensionType.CONTAINER_OF;
			boolean isExclusive = true;
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				if (!edge.getValue().toString().equals(dimension.getLabel())) {
					isExclusive = false;
					break;
				}
			}
			if (isExclusive) {
				for (NEDCMessageWrapper msg : messages) {
					if (!msg.getEdgeValue().toString().equals(dimension.getLabel())) {
						isExclusive = false;
						break;
					}
				}
			}
			if (isExclusive) {
				aggregate(LongSumAggregator.class.getName(), new LongWritable(1));				
			}
		}
	}
	
	
	public static class AggregateResult extends BasicComputation<LongWritable, Text, Text, NEDCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<metrics.nedc.NEDCMessageWrapper> messages)
				throws IOException {
			long sum = Long.valueOf(getAggregatedValue(LongSumAggregator.class.getName()).toString());
			NumberFormat formatter = new DecimalFormat("#0.00");
			String format = formatter.format((double) sum / getTotalNumVertices());
			vertex.setValue(new Text(format));
			vertex.voteToHalt();
		}
		
	}
	
	public static class MasterCompute extends DefaultMasterCompute {

		public void initialize() throws InstantiationException, IllegalAccessException {
			registerAggregator(LongSumAggregator.class.getName(), LongSumAggregator.class);
		}

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(SendOutEdges.class);
			} else if (superstep == 1) {
				setComputation(NEDCComputation.class);
			} else if (superstep == 3) {
				setComputation(AggregateResult.class);
			}
		}
	}
}
