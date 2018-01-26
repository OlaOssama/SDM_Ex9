package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.LinkedList;
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
	 *            - Child operator used to process next calls, usually SendOperator
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
		System.out.println("Receive " + nodeId + " open");
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take
		// multi-threading into
		// account where applicable!

		// init local cache
		// Attention: call open on child after starting receive server, so that
		// sendOperator can connect

		try {
			localCache = new LinkedList<AbstractRecord>();
			receiveServer = new TCPServer(listenerPort, localCache, finishedPeers);
			child.open();
//			receiveServer.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static boolean runServerFlag = false;

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take
		// multi-threading into
		// account where applicable!
		// process local and received records...

		// check if we finished processing of all records - hint: you can use
		// this.finishedPeers
		AbstractRecord rec = child.next();
		if (rec != null) { // receive from child
			return rec;
		} else { // receive from peer
			if (!runServerFlag) {
				System.out.println("receive "+nodeId + " run server");
				receiveServer.run(); // TODO: program blocked right after call this! -> fix it!
			}
//			System.out.println("aa");
			while (localCache.isEmpty()); // waiting for local cache to be filled
//			System.out.println("bb");
			return localCache.remove();
		}
	}

	@Override
	public void close() {
		System.out.println("Receive " + nodeId + " close");
		// TODO: implement this method
		receiveServer.stopServer();
		child.close();
		// reverse what was done in open()
	}

}
