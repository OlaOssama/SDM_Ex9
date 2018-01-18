package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;

import de.tuda.sdm.dmdb.net.TCPClient;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SendBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of send operator
 * @author melhindi
 *
 */
public class Send extends SendBase {

	/**
	 * Constructor of Send
	 * @param child - Child operator used to process next calls, e.g., TableScan or Selection
	 * @param nodeId - Own nodeId to identify which records to keep locally
	 * @param nodeMap - Map containing connection information (as "IP:port" or "domain-name:port") to establish connection to other peers
	 * @param partitionColumn - Number of column that should be used to repartition the data
	 */
	public Send(Operator child, int nodeId, Map<Integer,String> nodeMap, int partitionColumn) {
		super(child, nodeId, nodeMap, partitionColumn);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// init child

		// create a client socket for all peer nodes using information in nodeMap
		// store client socket in map for later use

	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// retrieve next from child and determine whether to keep record local or send to peer

				// store locally

				// send to a peer


		// reached end, close connections to peers


		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open() - hint there is a helper method that you can use
	}

}
