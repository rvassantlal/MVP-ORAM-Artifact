package benchmark;

import worker.IProcessingResult;

public class ThroughputLatencyMeasurement implements IProcessingResult {
	private final int treeHeight;
	private final double throughput;
	private final double latency;

	public ThroughputLatencyMeasurement(int treeHeight, double throughput, double latency) {
		this.treeHeight = treeHeight;
		this.throughput = throughput;
		this.latency = latency;
	}

	public int getTreeHeight() {
		return treeHeight;
	}

	public double getThroughput() {
		return throughput;
	}

	public double getLatency() {
		return latency;
	}

	@Override
	public String toString() {
		return "Tree Height: " + treeHeight + "\tThroughput: " + throughput + " ops/sec\tLatency: " + latency + " ms";
	}
}
