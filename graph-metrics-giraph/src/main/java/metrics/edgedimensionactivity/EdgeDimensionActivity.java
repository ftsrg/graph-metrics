package metrics.edgedimensionactivity;

import java.io.IOException;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.DimensionType;

public class EdgeDimensionActivity {

	public static class EdgeDimensionActivityComputation
			extends BasicComputation<LongWritable, Text, Text, LongWritable> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages)
				throws IOException {
			if (getSuperstep() == 0) {
				for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
					if (edge.getValue().toString().equals(DimensionType.OUTGOING.getLabel())) {
						aggregate(LongSumAggregator.class.getName(), new LongWritable(1));
					}
				}
			} else if (getSuperstep() == 1) {
				vertex.setValue(new Text(String.valueOf(getAggregatedValue(LongSumAggregator.class.getName()))));
			}
		}

	}

	public static class MasterCompute extends DefaultMasterCompute {

		public void initialize() throws InstantiationException, IllegalAccessException {
			registerAggregator(LongSumAggregator.class.getName(), LongSumAggregator.class);
		}

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 2) {
				haltComputation();
			}
		}

	}
}
