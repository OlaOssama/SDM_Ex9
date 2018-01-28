package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.print.attribute.standard.Finishings;

import de.tuda.sdm.dmdb.net.TCPServer;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.ReceiveBase;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of receive operator
 * 
 * @author melhindi
 *
 */
public class Receive extends ReceiveBase {
	private Thread t1;
	private Thread t2;

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
			t1 = new Thread(receiveServer);
			t1.start();

			child.open();

			Runnable task2 = () -> { // receive direkt from local
				AbstractRecord rec;
				do {
					rec = child.next();
					localCache.add(rec);
				} while (rec != null);
			};

			t2 = new Thread(task2);
			t2.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
		
		AbstractRecord rec = null;
		while (localCache.isEmpty()) {
			if (finishedPeers.get() == numPeers)
				break;
		}
		if (localCache.isEmpty())
			return null;
		rec = localCache.remove();
		return rec;
	}

	@Override
	public void close() {
		System.out.println("Receive " + nodeId + " close");
		// TODO: implement this method
		
		receiveServer.stopServer();
		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		child.close();
		// reverse what was done in open()
	}

}
