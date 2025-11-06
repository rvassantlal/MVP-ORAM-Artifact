package benchmark;

import controller.IBenchmarkStrategy;
import controller.IWorkerStatusListener;
import controller.WorkerHandler;
import generic.ResourcesMeasurements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.IProcessingResult;
import worker.ProcessInformation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MeasurementBenchmarkStrategy implements IBenchmarkStrategy, IWorkerStatusListener {
	private final Logger logger = LoggerFactory.getLogger("benchmarking");
	private final Lock lock;
	private final Condition sleepCondition;
	private boolean measureResources;
	private WorkerHandler[] serverWorkers;
	private WorkerHandler[] clientWorkers;
	private int round;
	private String storageFilenamePrefix;
	private String performanceFileNamePrefix;
	private CountDownLatch workersReadyCounter;
	private CountDownLatch clientsEndedCounter;
	private CountDownLatch measurementDeliveredCounter;
	private final String serverCommand;
	private final String proxyCommand;
	private final String clientCommand;
	private final String sarCommand;
	private final Set<Integer> serverWorkersIds;
	private final Set<Integer> clientWorkersIds;
	private final Map<Integer, WorkerHandler> measurementWorkers;
	private int testLength;
	private final LinkedList<ThroughputLatencyMeasurement> throughputLatencyMeasurements;
	private File rawDataDir;
	private File processedDataDir;

	//Storing data for plotting
	private double[] latencyValues;
	private double[] throughputValues;

	public MeasurementBenchmarkStrategy() {
		this.lock = new ReentrantLock(true);
		this.sleepCondition = lock.newCondition();
		this.serverWorkersIds = new HashSet<>();
		this.clientWorkersIds = new HashSet<>();
		this.measurementWorkers = new HashMap<>();
		this.throughputLatencyMeasurements = new LinkedList<>();
		//String initialCommand = "java -Xmx8G -Dlogback.configurationFile=./config/logback.xml -cp lib/* ";
		String initialCommand = "java -Dlogback.configurationFile=./config/logback.xml -cp lib/* ";
		this.serverCommand = initialCommand + "TaoServer.TaoServer --unit ";
		this.proxyCommand = initialCommand + "TaoProxy.TaoProxy --unit ";
		this.clientCommand = initialCommand + "TaoClient.TaoClient --runType load_test --rwRatio 0.5 " +
				"--warmup_operations 10 --quorum_type random ";
		this.sarCommand = "sar -u -r -n DEV 1";
	}

	@Override
	public void executeBenchmark(WorkerHandler[] workers, Properties benchmarkParameters) {
		long startTime = System.currentTimeMillis();

		int[] faultThresholds = Arrays.stream(benchmarkParameters.getProperty("fault_thresholds").split(" ")).mapToInt(Integer::parseInt).toArray();
		String serverIpsInput = benchmarkParameters.getProperty("server_ips", "");
		int[] clientsPerRound = Arrays.stream(benchmarkParameters.getProperty("clients_per_round").split(" ")).mapToInt(Integer::parseInt).toArray();
		measureResources = Boolean.parseBoolean(benchmarkParameters.getProperty("measure_resources"));
		testLength = Integer.parseInt(benchmarkParameters.getProperty("measurement_duration")) * 1000;
		int writeBackThreshold = Integer.parseInt(benchmarkParameters.getProperty("write_back_threshold"));
		int bucketSize = Integer.parseInt(benchmarkParameters.getProperty("bucket_sizes"));
		int blockSize = Integer.parseInt(benchmarkParameters.getProperty("block_sizes"));
		int storageSize = Integer.parseInt(benchmarkParameters.getProperty("storage_size"));
		String[] zipfParametersStr = benchmarkParameters.getProperty("zipf_parameters").split(" ");
		double zipfParameter = Double.parseDouble(zipfParametersStr[0]);//Use the first one
		String outputPath = benchmarkParameters.getProperty("output.path", ".");

		File outputDir = new File(outputPath, "output");
		rawDataDir = new File(outputDir, "raw_data");
		processedDataDir = new File(outputDir, "processed_data");
		createFolderIfNotExist(rawDataDir);
		createFolderIfNotExist(processedDataDir);

		int sleepBetweenRounds = 30;

		int strategyParameterIndex = 1;
		int nStrategyParameters = faultThresholds.length;
		try {
			for (int faultThresholdIndex = 0; faultThresholdIndex < faultThresholds.length; faultThresholdIndex++) {
				int faultThreshold = faultThresholds[faultThresholdIndex];
				int nServers = faultThreshold * 2 + 1;

				//Separate the workers
				serverWorkers = new WorkerHandler[nServers];
				clientWorkers = new WorkerHandler[workers.length - nServers];
				System.arraycopy(workers, 0, serverWorkers, 0, serverWorkers.length);
				System.arraycopy(workers, nServers, clientWorkers, 0, clientWorkers.length);

				//Sort client workers to use the same worker as measurement client
				Arrays.sort(clientWorkers, (o1, o2) -> -Integer.compare(o1.getWorkerId(), o2.getWorkerId()));

				serverWorkersIds.clear();
				clientWorkersIds.clear();
				Arrays.stream(serverWorkers).forEach(w -> serverWorkersIds.add(w.getWorkerId()));
				Arrays.stream(clientWorkers).forEach(w -> clientWorkersIds.add(w.getWorkerId()));

				String serverIps;

				//Setup workers
				if (serverIpsInput.isEmpty()) {
					serverIps = generateLocalhostIPs(nServers);
				} else {
					serverIps = selectServerIPs(serverIpsInput, nServers);
				}

				//Setup workers
				logger.info("Setting up workers...");
				String setupInformation = String.format("%d\n%d\n%d\n%d\n%s",
						writeBackThreshold,
						blockSize,
						bucketSize,
						storageSize,
						serverIps);
				Arrays.stream(workers).forEach(w -> w.setupWorker(setupInformation));

				printWorkersInfo();

				logger.info("============ Strategy Parameters: {} out of {} ============",
						strategyParameterIndex, nStrategyParameters);
				logger.info("Fault Threshold: {}", faultThreshold);
				logger.info("Servers: {}", serverIps.replaceAll("\n", " "));
				logger.info("Write Back Threshold: {}", writeBackThreshold);
				logger.info("Bucket Size: {}", bucketSize);
				logger.info("Block Size: {}", blockSize);
				logger.info("Storage Size: {}", storageSize);
				logger.info("Clients per round: {}", Arrays.toString(clientsPerRound));

				latencyValues = new double[clientsPerRound.length];
				throughputValues = new double[clientsPerRound.length];

				performanceFileNamePrefix = String.format("f_%d_bucket_%d_block_%d_zipf_%s_",
						faultThreshold, bucketSize, blockSize, zipfParametersStr[0]);

				round = 1;
				while (true) {
					try {
						lock.lock();
						logger.info("============ Round: {} ============", round);
						measurementWorkers.clear();
						throughputLatencyMeasurements.clear();

						if (measureResources) {
							measurementWorkers.put(serverWorkers[0].getWorkerId(), serverWorkers[0]);
						}

						int nClients = clientsPerRound[round - 1];
						storageFilenamePrefix = String.format("f_%d_bucket_%d_block_%d_zipf_%s_round_%d_",
								faultThreshold, bucketSize, blockSize, zipfParametersStr[0], nClients);

						//Distribute clients per workers
						int[] clientsPerWorker = distributeClientsPerWorkers(clientWorkers.length, nClients);
						String vector = Arrays.toString(clientsPerWorker);
						int total = Arrays.stream(clientsPerWorker).sum();
						logger.info("Clients per worker: {} -> Total: {}", vector, total);

						//Start servers
						startServers(serverWorkers);
						sleepSeconds(2);

						//Start proxy
						startProxy(serverWorkers);
						sleepSeconds(2);

						//Start clients
						startClients(clientWorkers, clientsPerWorker, zipfParameter);

						if (measureResources) {
							startResourceMeasurements(serverWorkers[0], clientWorkers[0]);
						}

						//Get measurements
						getMeasurements(clientsPerWorker.length);

						//Stop processes
						Arrays.stream(workers).forEach(WorkerHandler::stopWorker);

						if (faultThresholdIndex < faultThresholds.length - 1 || round < clientsPerRound.length) {
							//Wait before next execution
							logger.info("Waiting {}s before next execution", sleepBetweenRounds);
							sleepSeconds(sleepBetweenRounds);
						}

						if (round == clientsPerRound.length) {
							break;
						}
						round++;
					} finally {
						lock.unlock();
					}
				}

				storeProcessedResults(clientsPerRound);
			}

			long endTime = System.currentTimeMillis();
			logger.info("Execution duration: {}s", (endTime - startTime) / 1000);
		} catch(InterruptedException e){
			logger.error("Benchmark interrupted", e);
		}
	}

	private void storeProcessedResults(int[] clientsPerRound) {
		String fileName = performanceFileNamePrefix + "throughput_latency_results.dat";
		Path path = Paths.get(processedDataDir.getPath(), fileName);
		try (BufferedWriter resultFile = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path)))) {
			resultFile.write("#clients throughput[ops/s] latency[ms]\n");
			for (int i = 0; i < clientsPerRound.length; i++) {
				resultFile.write(String.format("%d %.3f %.3f\n", clientsPerRound[i],
						throughputValues[i], latencyValues[i]));
			}
		} catch (IOException e) {
			logger.error("Error while storing processed results", e);
		}
	}

	private String selectServerIPs(String serverIps, int nServers) {
		StringBuilder sb = new StringBuilder();
		String[] ips = serverIps.split(" ");
		if (ips.length < nServers) {
			logger.warn("Not enough server IPs provided. Using localhost for remaining servers.");
		}
		for (int i = 0; i < nServers; i++) {
			if (i < ips.length) {
				sb.append(ips[i]).append("\n");
			} else {
				sb.append("127.0.0.1\n");
			}
		}
		return sb.substring(0, sb.length() - 1);
	}

	private String generateLocalhostIPs(int nServers) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < nServers; i++) {
			sb.append("127.0.0.1\n");
		}
		return sb.substring(0, sb.length() - 1);
	}

	private void createFolderIfNotExist(File dir) {
		if (!dir.exists()) {
			boolean isCreated = dir.mkdirs();
			if (!isCreated) {
				logger.error("Could not create results directory: {}", dir);
			}
		}
	}

	private void startServers(WorkerHandler[] serverWorkers) throws InterruptedException {
		logger.info("Starting servers...");

		for (int i = 0; i < serverWorkers.length; i++) {
			String command = serverCommand + i;
			ProcessInformation[] commands = {
					new ProcessInformation(command, ".")
			};
			serverWorkers[i].startWorker(0, commands, this);
		}
	}

	private void startProxy(WorkerHandler[] serverWorkers) throws InterruptedException {
		logger.info("Starting proxies (this will take some time)...");
		workersReadyCounter = new CountDownLatch(serverWorkers.length);
		for (int i = 0; i < serverWorkers.length; i++) {
			String command = proxyCommand + i;
			ProcessInformation[] commands = {
					new ProcessInformation(command, ".")
			};
			serverWorkers[i].startWorker(0, commands, this);
		}
		workersReadyCounter.await();
	}

	private void startClients(WorkerHandler[] clientWorkers, int[] clientsPerWorker, double zipf) throws InterruptedException {
		logger.info("Starting clients...");
		workersReadyCounter = new CountDownLatch(clientsPerWorker.length);
		clientsEndedCounter = new CountDownLatch(clientsPerWorker.length);

		int clientId = 1;
		for (int i = 0; i < clientsPerWorker.length; i++) {
			measurementWorkers.put(clientWorkers[i].getWorkerId(), clientWorkers[i]);
			int nClients = clientsPerWorker[i];
			String command = clientCommand + "--zipfExp " + zipf + " --clients " + nClients + " --id " + clientId
					+ " --load_test_length " + testLength;
			ProcessInformation[] commands = {
					new ProcessInformation(command, ".")
			};
			clientWorkers[i].startWorker(0, commands, this);
			logger.debug("Using client worker {}", clientWorkers[i].getWorkerId());
			sleepSeconds(3);
			clientId += nClients;
		}

		workersReadyCounter.await();
	}

	private void startResourceMeasurements(WorkerHandler... workers) throws InterruptedException {
		logger.info("Starting resource measurements...");
		workersReadyCounter = new CountDownLatch(workers.length);
		for (WorkerHandler worker : workers) {
			measurementWorkers.put(worker.getWorkerId(), worker);
			ProcessInformation[] commands = {
					new ProcessInformation(sarCommand, ".")
			};
			worker.startWorker(0, commands, this);
		}
		workersReadyCounter.await();
	}

	private void getMeasurements(int nUsedClientWorkers) throws InterruptedException {
		//Start measurements
		logger.debug("Starting measurements...");
		measurementWorkers.values().forEach(WorkerHandler::startProcessing);

		//Wait for measurements
		logger.debug("Waiting for measurements...");
		clientsEndedCounter.await();

		//Stop measurements
		measurementWorkers.values().forEach(WorkerHandler::stopProcessing);

		logger.info("Getting measurements...");
		int nMeasurements = nUsedClientWorkers;
		if (measureResources) {
			nMeasurements += 2;//1 server and 1 client
		}

		logger.debug("Getting {} measurements from {} workers...", nMeasurements, measurementWorkers.size());
		measurementDeliveredCounter = new CountDownLatch(nMeasurements);
		measurementWorkers.values().forEach(WorkerHandler::requestProcessingResult);

		measurementDeliveredCounter.await();
		printThroughputLatencyMeasurements();
		storeThroughputLatencyMeasurements();
	}

	private void printThroughputLatencyMeasurements() {
		double totalThroughput = 0;
		double averageLatency = 0;
		int height = throughputLatencyMeasurements.get(0).getTreeHeight();
		for (ThroughputLatencyMeasurement measurement : throughputLatencyMeasurements) {
			totalThroughput += measurement.getThroughput();
			averageLatency += measurement.getLatency();
		}
		averageLatency /= throughputLatencyMeasurements.size();
		latencyValues[round - 1] = averageLatency;
		throughputValues[round - 1] = totalThroughput;
		logger.info("Tree Height: {}", height);
		logger.info("Total throughput: {} ops/s", totalThroughput);
		logger.info("Average latency: {} ms", averageLatency);
	}

	private int[] distributeClientsPerWorkers(int nClientWorkers, int nClients) {
		if (nClients <= nClientWorkers) {
			int[] distribution = new int[nClients];
			Arrays.fill(distribution, 1);
			return distribution;

		}
		int[] distribution = new int[nClientWorkers];
		int nClientPerWorker = nClients / nClientWorkers;

		Arrays.fill(distribution, nClientPerWorker);

		int remainingClients = nClients - nClientPerWorker * distribution.length;
		int i = 1;
		while (remainingClients > 0) {
			distribution[i]++;
			remainingClients--;
			i = (i + 1) % nClientWorkers;
		}
		return distribution;
	}

	@Override
	public synchronized void onReady(int workerId) {
		logger.debug("Worker {} is ready", workerId);
		workersReadyCounter.countDown();
	}

	@Override
	public void onEnded(int workerId) {
		if (clientWorkersIds.contains(workerId)) {
			logger.debug("Client worker {} has ended", workerId);
			clientsEndedCounter.countDown();
		}
	}

	@Override
	public void onError(int workerId, String errorMessage) {
		if (serverWorkersIds.contains(workerId)) {
			logger.error("Error in server worker {}: {}", workerId, errorMessage);
		} else if (clientWorkersIds.contains(workerId)) {
			logger.error("Error in client worker {}: {}", workerId, errorMessage);
		} else {
			logger.error("Error in unused worker {}: {}", workerId, errorMessage);
		}
	}

	@Override
	public synchronized void onResult(int workerId, IProcessingResult processingResult) {
		if (processingResult instanceof ResourcesMeasurements) {
			String tag = null;
			if (workerId == serverWorkers[0].getWorkerId()) {
				logger.debug("Processing resource measurement from server worker {}", workerId);
				tag = "server";
			} else if (workerId == clientWorkers[0].getWorkerId()) {
				logger.debug("Processing resource measurement from client worker {}", workerId);
				tag = "client";
			}
			if (tag != null) {
				ResourcesMeasurements measurements = (ResourcesMeasurements) processingResult;
				processResourceMeasurements(tag, measurements);
				measurementDeliveredCounter.countDown();
			}
		} else if (processingResult instanceof ThroughputLatencyMeasurement) {
			ThroughputLatencyMeasurement measurement = (ThroughputLatencyMeasurement) processingResult;
			logger.debug("Processing throughput and latency measurement from worker {}: {}", workerId, measurement);
			throughputLatencyMeasurements.add(measurement);
			measurementDeliveredCounter.countDown();
		} else {
			logger.warn("Unknown processing result from worker {}", workerId);
		}
	}

	private void processResourceMeasurements(String tag, ResourcesMeasurements measurements) {
		long[] cpu = measurements.getCpu();
		long[] memory = measurements.getMemory();
		long[][] netReceived = measurements.getNetReceived();
		long[][] netTransmitted = measurements.getNetTransmitted();

		String filename = storageFilenamePrefix + "cpu_" + tag + ".csv";
		saveResourcesMeasurements(filename, cpu);

		filename = storageFilenamePrefix + "mem_" + tag + ".csv";
		saveResourcesMeasurements(filename, memory);

		filename = storageFilenamePrefix + "net_received_" + tag + ".csv";
		saveResourcesMeasurements(filename, netReceived);

		filename = storageFilenamePrefix + "net_transmitted_" + tag + ".csv";
		saveResourcesMeasurements(filename, netTransmitted);
	}

	private void storeThroughputLatencyMeasurements() {
		String filename = storageFilenamePrefix + "client_global.csv";
		Path path = Paths.get(rawDataDir.getAbsolutePath(), filename);
		try (BufferedWriter resultFile = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path)))) {
			resultFile.write("throughput(ops/s),latency(ms),height\n");
			for (ThroughputLatencyMeasurement measurement : throughputLatencyMeasurements) {
				resultFile.write(Double.toString(measurement.getThroughput()));
				resultFile.write(",");
				resultFile.write(Double.toString(measurement.getLatency()));
				resultFile.write(",");
				resultFile.write(Integer.toString(measurement.getTreeHeight()));
				resultFile.write("\n");
			}
			resultFile.flush();
		} catch (IOException e) {
			logger.error("Error while storing throughput and latency measurements", e);
		}

	}

	private void saveResourcesMeasurements(String filename, long[]... data) {
		Path path = Paths.get(rawDataDir.getAbsolutePath(), filename);
		try (BufferedWriter resultFile = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path)))) {
			int size = data[0].length;
			int i = 0;
			while (i < size) {
				StringBuilder sb = new StringBuilder();
				for (long[] datum : data) {
					sb.append(String.format("%.2f", datum[i] / 100.0));
					sb.append(",");
				}
				sb.deleteCharAt(sb.length() - 1);
				resultFile.write(sb + "\n");
				i++;
			}
			resultFile.flush();
		} catch (IOException e) {
			logger.error("Error while storing resources measurements results", e);
		}
	}

	private void sleepSeconds(long duration) throws InterruptedException {
		lock.lock();
		ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		scheduledExecutorService.schedule(() -> {
			lock.lock();
			sleepCondition.signal();
			lock.unlock();
		}, duration, TimeUnit.SECONDS);
		sleepCondition.await();
		scheduledExecutorService.shutdown();
		lock.unlock();
	}

	private void printWorkersInfo() {
		StringBuilder sb = new StringBuilder();
		for (WorkerHandler serverWorker : serverWorkers) {
			sb.append(serverWorker.getWorkerId());
			sb.append(" ");
		}
		logger.info("Server workers[{}]: {}", serverWorkers.length, sb);

		sb = new StringBuilder();
		for (WorkerHandler clientWorker : clientWorkers) {
			sb.append(clientWorker.getWorkerId());
			sb.append(" ");
		}
		logger.info("Client workers[{}]: {}", clientWorkers.length, sb);
	}
}
