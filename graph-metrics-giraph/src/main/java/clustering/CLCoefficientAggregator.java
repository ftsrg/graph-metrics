package clustering;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

import clustering.ClusteringCoefficient.ClusteringCoefficientComputation;
import clustering.ClusteringCoefficient.SendFriendsList;

public class CLCoefficientAggregator extends DefaultMasterCompute { // master
																	// compute
	// implementations are usually extending DefaultMasterCompute abstract class
	// just the same way that our basic BSP compute implementations extended
	// BasicComputation. Unlike BasicComputation you have to provide
	// implementations
	// for two methods: compute() and initialize()

	public static final String CL_COEFFICIENT_AGGREGATOR = "global.clustering.coefficient";
	public static String COUNTER_GROUP = "Clustering Coefficient";
	public static String COUNTER_NAME = "Global (x1000)";

	@Override
	public final void compute() {
		long superstep = getSuperstep();
		if (superstep == 0) {
			setComputation(SendFriendsList.class);
		} else {
			setComputation(ClusteringCoefficientComputation.class);
		}
		if (superstep == 2) {
			double partialSum = ((DoubleWritable) getAggregatedValue(CL_COEFFICIENT_AGGREGATOR)).get();
			double globalCoefficient = partialSum / (double) getTotalNumVertices();
			Counters.updateCounter(getContext(), COUNTER_GROUP, COUNTER_NAME, (long) (1000 * globalCoefficient));
		}
	}

	@Override
	// this method gets called during overal initialization of the Giraph
	// BSP machinery. It is an ideal place to register all required
	// aggregators.
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(CL_COEFFICIENT_AGGREGATOR, LongSumAggregator.class); // this is how we
															// associate
		// an aggregator ID with a particular implementation of an aggregator:
		// in our case we are using a built-in LongSumAggregator that simply
		// sums all of the values sent to it
	}
}