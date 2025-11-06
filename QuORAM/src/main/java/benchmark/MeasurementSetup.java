package benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.ISetupWorker;

import java.io.FileWriter;
import java.io.IOException;

public class MeasurementSetup implements ISetupWorker {
	private final Logger logger = LoggerFactory.getLogger("benchmarking");

	@Override
	public void setup(String setupInformation) {
		String[] args = setupInformation.split("\n");
		int writeBackThreshold = Integer.parseInt(args[0]);
		int blockSize = Integer.parseInt(args[1]);
		int bucketSize = Integer.parseInt(args[2]);
		int storageSize = Integer.parseInt(args[3]);
		String[] servers = new String[args.length - 4];
		System.arraycopy(args, 4, servers, 0, args.length - 4);
		String config = createConfig(servers, writeBackThreshold, blockSize, bucketSize, storageSize);

		String fileName = "config.properties";

		try (FileWriter myWriter = new FileWriter(fileName)){
			myWriter.write(config);
			logger.debug("Successfully wrote to the file.");
		} catch (IOException e) {
			logger.error("An error occurred.", e);
		}
	}

	private String createConfig(String[] servers, int writeBackThreshold, int blockSize,
								int bucketSize, int storageSize) {
		StringBuilder builder = new StringBuilder();

		builder.append("oram_file=oram.txt\n\n");
		builder.append("proxy_thread_count=10\n\n");
		builder.append("write_back_threshold=").append(writeBackThreshold).append("\n\n");
		builder.append("client_port=12337\n\n");

		for (int i = 0; i < servers.length; i++) {
			String ip = servers[i];
			builder.append("proxy_hostname").append(i).append("=").append(ip).append("\n");
			builder.append("proxy_port").append(i).append("=" + (12339 + i * 10) + "\n");
			builder.append("server_hostname").append(i).append("=").append(ip).append("\n");
			builder.append("server_port").append(i).append("=" + (12338 + i * 10) + "\n\n");
		}

		builder.append("block_size=").append(blockSize).append("\n\n");
		builder.append("blocks_in_bucket=").append(bucketSize).append("\n\n");
		builder.append("block_meta_data_size=18\n\n");
		builder.append("iv_size=16\n\n");
		builder.append("min_server_size=").append(storageSize).append("\n\n");
		builder.append("num_storage_servers=1\n\n");
		builder.append("num_oram_units=").append(servers.length).append("\n\n");
		builder.append("incomplete_cache_limit=1000\n\n");
		builder.append("max_client_id=10000\n\n");
		builder.append("proxy_service_threads=128\n\n");
		builder.append("access_daemon_delay=5000\n\n");
		builder.append("client_timeout=10000\n\n");

		return builder.toString();
	}
}
