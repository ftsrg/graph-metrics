package metrics.degrees;

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

public class AverageDegree {

	public static class AverageDegreeComputation extends BasicComputation<LongWritable, Text, Text, LongWritable> {
		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages)
				throws IOException {
			if (getSuperstep() == 0) {
				aggregate("nodeNumbers", new LongWritable(1));
				aggregate("edgeNumbers", new LongWritable(vertex.getNumEdges()));
			} else if (getSuperstep() == 1) {
				double numberOfNodes = Double.parseDouble(getAggregatedValue("nodeNumbers").toString());
				double numberOfEdges = Double.parseDouble(getAggregatedValue("edgeNumbers").toString());
				vertex.setValue(new Text(String.valueOf(2 * numberOfEdges / numberOfNodes)));
			}

		}
	}

	public static class MasterCompute extends DefaultMasterCompute {

		public void initialize() throws InstantiationException, IllegalAccessException {
			registerAggregator("nodeNumbers", LongSumAggregator.class);
			registerAggregator("edgeNumbers", LongSumAggregator.class);
		}

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(AverageDegreeComputation.class);
			} else if (superstep == 1) {
				haltComputation();
			}
		}
	}

}
