package benchmark;

import generic.IMeasurementEventProcessor;
import generic.ResourcesMeasurementEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.IProcessingResult;
import worker.IWorkerEventProcessor;

public class MeasurementEventProcessor implements IWorkerEventProcessor {
	private final Logger logger = LoggerFactory.getLogger("benchmarking");

	private static final String PROXY_READY_PATTERN = "Finished init, running proxy";
	private static final String CLIENT_READY_PATTERN = "Beginning load test";
	private static final String CLIENT_STARTED_PATTERN = "Tree Height";
	private static final String CLIENT_END_PATTERN = "Throughput:";
	private static final String SAR_READY_PATTERN = "%";

	private IMeasurementEventProcessor measurementEventProcessor;
	private boolean isReady;
	private boolean isEnded;
	private boolean doMeasurement;

	@Override
	public void process(String line) {
		logger.debug(line);
		if (!isReady) {
			if (line.contains(PROXY_READY_PATTERN)) {
				isReady = true;
			} else if (line.contains(CLIENT_READY_PATTERN)) {
				isReady = true;
			} else if (line.contains(SAR_READY_PATTERN)) {
				isReady = true;
				measurementEventProcessor = new ResourcesMeasurementEventProcessor();
			}
		}
		if (line.contains(CLIENT_STARTED_PATTERN)) {
			measurementEventProcessor = new ThroughputLatencyMeasurementEventProcessor();
		}
		if (measurementEventProcessor instanceof ResourcesMeasurementEventProcessor && doMeasurement) {
			measurementEventProcessor.process(line);
		} else if (measurementEventProcessor instanceof ThroughputLatencyMeasurementEventProcessor) {
			measurementEventProcessor.process(line);
		}

		if (!isEnded) {
			if (line.contains(CLIENT_END_PATTERN)) {
				isEnded = true;
			}
		}
	}

	@Override
	public void startProcessing() {
		logger.debug("Measuring");
		if (measurementEventProcessor != null) {
			measurementEventProcessor.reset();
		}
		doMeasurement = true;
	}

	@Override
	public void stopProcessing() {
		logger.debug("Not Measuring");
		doMeasurement = false;
	}

	@Override
	public IProcessingResult getProcessingResult() {
		if (measurementEventProcessor != null) {
			IProcessingResult result = measurementEventProcessor.getResult();
			logger.debug("Result: " + result);
			return result;
		} else {
			return null;
		}
	}

	@Override
	public boolean isReady() {
		return isReady;
	}

	@Override
	public boolean ended() {
		return isEnded;
	}
}
