package benchmark;

import generic.IMeasurementEventProcessor;
import worker.IProcessingResult;

public class ThroughputLatencyMeasurementEventProcessor implements IMeasurementEventProcessor {
	private static final String THROUGHPUT_PATTERN = "Throughput:";
	private static final String LATENCY_PATTERN = "Average response time was";
	private static final String TREE_HEIGHT_PATTERN = "Tree Height:";
	private double throughput = -1;
	private double latency = -1;
	private int treeHeight = -1;
	@Override
	public void process(String line) {
		if (line.contains(THROUGHPUT_PATTERN)) {
			String[] tokens = line.split(" ");
			throughput = Double.parseDouble(tokens[tokens.length - 1]);
		} else if (line.contains(LATENCY_PATTERN)) {
			String[] tokens = line.split(" ");
			latency = Double.parseDouble(tokens[tokens.length - 2]);
		} else if (line.contains(TREE_HEIGHT_PATTERN)) {
			String[] tokens = line.split(" ");
			treeHeight = Integer.parseInt(tokens[tokens.length - 1]);
		}
	}

	@Override
	public void reset() {
	}

	@Override
	public IProcessingResult getResult() {
		return new ThroughputLatencyMeasurement(treeHeight, throughput, latency);
	}
}
