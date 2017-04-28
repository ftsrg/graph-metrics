package metrics.clustering;

import org.apache.hadoop.io.LongWritable;

import util.MessageWrapper;

public class LongIdFriendsList extends MessageWrapper<LongWritable, LongArrayListWritable> {

	@Override
	public Class<LongWritable> getVertexIdClass() {
		return LongWritable.class;
	}

	@Override
	public Class<LongArrayListWritable> getMessageClass() {
		return LongArrayListWritable.class;
	}

}
