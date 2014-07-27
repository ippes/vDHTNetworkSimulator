package net.tomp2p.vdht;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class creating and appending results of simulation runs.
 * 
 * @author Seppi
 */
public final class Outcome {

	private static final Logger logger = LoggerFactory.getLogger(Outcome.class);

	// outcome file name
	private static final String outcomeFileName = "outcome.csv";

	private static void loadFile() {
		File file = new File(outcomeFileName);
		if (!file.exists()) {
			logger.debug("Creating an outcome.csv file.");
			try {
				FileWriter fileWriter = new FileWriter(file);

				fileWriter.append("date");
				fileWriter.append(',');
				fileWriter.append("time");
				fileWriter.append(',');
				fileWriter.append("runtimeInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("numPeersMin");
				fileWriter.append(',');
				fileWriter.append("numPeersMax");
				fileWriter.append(',');
				fileWriter.append("numPuts");
				fileWriter.append(',');
				fileWriter.append("putStrategyName");
				fileWriter.append(',');
				fileWriter.append("churnStrategyName");
				fileWriter.append(',');
				fileWriter.append("replication");
				fileWriter.append(',');
				fileWriter.append("replicationFactor");
				fileWriter.append(',');
				fileWriter.append("replicationIntervalInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("putTTLInSeconds");
				fileWriter.append(',');
				fileWriter.append("putPrepareTTLInSeconds");
				fileWriter.append(',');
				fileWriter.append("ttlCheckIntervalInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("numKeys");
				fileWriter.append(',');
				fileWriter.append("putConcurrencyFactor");
				fileWriter.append(',');
				fileWriter.append("putDelayMinInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("putDelayMaxInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("churnRateJoin");
				fileWriter.append(',');
				fileWriter.append("churnRateLeave");
				fileWriter.append(',');
				fileWriter.append("churnJoinLeaveRate");
				fileWriter.append(',');
				fileWriter.append("churnRateMinDelayInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("churnRateMaxDelayInMilliseconds");
				fileWriter.append(',');
				fileWriter.append("presentVersions");
				fileWriter.append(',');
				fileWriter.append("versionWrites");
				fileWriter.append('\n');

				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				logger.error("Couldn't create outcome file.", e);
			}
		}
	}

	public static void writeResult(int presentVersions, int versionWrites) {
		// create file if necessary
		loadFile();
		try {
			logger.debug("Writing into outcome file.");

			FileWriter fileWriter = new FileWriter(outcomeFileName, true);

			Date date = new Date();
			// write time stamp
			fileWriter.append(new SimpleDateFormat("dd.MM.yy").format(date));
			fileWriter.append(',');
			fileWriter.append(new SimpleDateFormat("hh:mm:ss").format(date));
			fileWriter.append(',');
			// write configuration
			fileWriter.append(Integer.toString(Configuration.getRuntimeInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getNumPeersMin()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getNumPeersMax()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getNumPuts()));
			fileWriter.append(',');
			fileWriter.append(Configuration.getPutStrategyName());
			fileWriter.append(',');
			fileWriter.append(Configuration.getChurnStrategyName());
			fileWriter.append(',');
			fileWriter.append(Configuration.getReplication());
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getReplicationFactor()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getReplicationIntervalInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getPutTTLInSeconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getPutPrepareTTLInSeconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getTTLCheckIntervalInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getNumKeys()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getPutConcurrencyFactor()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getPutDelayMinInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getPutDelayMaxInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getChurnRateJoin()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getChurnRateLeave()));
			fileWriter.append(',');
			fileWriter.append(Double.toString(Configuration.getChurnJoinLeaveRate()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getChurnRateMinDelayInMilliseconds()));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(Configuration.getChurnRateMaxDelayInMilliseconds()));
			fileWriter.append(',');
			// write results
			fileWriter.append(Integer.toString(presentVersions));
			fileWriter.append(',');
			fileWriter.append(Integer.toString(versionWrites));
			fileWriter.append('\n');

			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			logger.error("Couldn't write configuration into outcome file.", e);
		}
	}
}
