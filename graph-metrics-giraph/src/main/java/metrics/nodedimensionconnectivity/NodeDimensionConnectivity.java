package metrics.nodedimensionconnectivity;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import metrics.clustering.LCCMessageWrapper;
import util.DimensionType;
import util.LongArrayListWritable;

public class NodeDimensionConnectivity {

	public static class SendOutEdges extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				LCCMessageWrapper messageWrapper = new LCCMessageWrapper();
				messageWrapper.setSourceId(vertex.getId());
				messageWrapper.setMessage(new LongArrayListWritable());
				messageWrapper.setEdgeValue(edge.getValue());
			}

		}

	}

	public static class NodeDimensionConnectivityComputation
			extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {
		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {
			boolean isActive = false;
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				if (edge.getValue().toString().equals(DimensionType.CONTAINER_OF.getLabel())) {
					isActive = true;
					break;
				}
			}
			if (!isActive) {
				for (LCCMessageWrapper mw : messages) {
					if (mw.getEdgeValue().toString().equals(DimensionType.CONTAINER_OF.getLabel())) {
						isActive = true;
						break;
					}
				}
			}
			if (isActive) {
				aggregate(LongSumAggregator.class.getName(), new LongWritable(1));
			}
		}

	}
	
	public static class SetVertexValues extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {
			String aggregatedValue = getAggregatedValue(LongSumAggregator.class.getName()).toString();
			NumberFormat formatter = new DecimalFormat("#0.00");
			double activeNodes = Double.parseDouble(aggregatedValue);
			String result = formatter.format(activeNodes / getTotalNumVertices());
			vertex.setValue(new Text(result));
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
				setComputation(NodeDimensionConnectivityComputation.class);
			} else if (superstep == 2) {
				setComputation(SetVertexValues.class);
			}
		}

	}
}