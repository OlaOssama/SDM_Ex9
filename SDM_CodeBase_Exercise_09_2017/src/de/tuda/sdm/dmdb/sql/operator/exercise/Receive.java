package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuda.sdm.dmdb.net.TCPServer;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.ReceiveBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of receive operator
 * 
 * @author melhindi
 *
 */
public class Receive extends ReceiveBase {

	/**
	 * Constructor of Receive
	 * 
	 * @param child
	 *            - Child operator used to process next calls, usually
	 *            SendOperator
	 * @param numPeers
	 *            - Number of peer nodes that have to finish processing before
	 *            operator finishes
	 * @param listenerPort
	 *            - Port on which to bind receive server
	 * @param nodeId
	 *            - Own nodeId, used for debugging
	 */
	public Receive(Operator child, int numPeers, int listenerPort, int nodeId) {
		super(child, numPeers, listenerPort, nodeId);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take
		// multi-threading into
		// account where applicable!

		// init local cache
		// Attention: call open on child after starting receive server, so that
		// sendOperator can connect

		// finishedPeers= new AtomicInteger(numPeers-1);
		
		try {
			receiveServer = new TCPServer(listenerPort, localCache, finishedPeers);
			child.open();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public AbstractRecord next() {
		System.out.println("Before server: " + receiveServer.getActiveConnectionsCount());
		receiveServer.run();
		System.out.println("After server: " + receiveServer.getActiveConnectionsCount());
		if (this.finishedPeers.get() == numPeers) {// receiveServer.getActiveConnectionsCount()
			return null;
		} else {
			return localCache.remove();
		}
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take
		// multi-threading into
		// account where applicable!
		// process local and received records...

		// check if we finished processing of all records - hint: you can use
		// this.finishedPeers
	}

	@Override
	public void close() {
		// TODO: implement this method
		child.close();
		receiveServer.stopServer();
		// reverse what was done in open()
	}

}
