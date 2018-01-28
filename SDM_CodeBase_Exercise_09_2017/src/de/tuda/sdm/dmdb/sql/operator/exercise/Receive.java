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
	private Thread t1; // thread adding records returned direct from child

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
			receiveServer.start();

			child.open();

			Runnable task2 = () -> { // receive direct from local
				AbstractRecord rec;
				do {
					rec = child.next();
					if (rec != null) {
						boolean lock = !(localCache.getClass().getPackage().getName().equals("java.util.concurrent"));
						boolean res;

						if (lock) {
							synchronized (localCache) {
								res = localCache.offer(rec);
							}
						} else {
							res = localCache.offer(rec);
						}

						if (!res)
							System.out.println("Adding to queue NOT ok");
					}
				} while (rec != null);
			};

			t1 = new Thread(task2);
			t1.start();

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
		while (localCache.isEmpty() && finishedPeers.get() < numPeers) {
		}

		//TODO: how to know that peers are finished sending?
		// eliminate this workaround
		if (finishedPeers.get() == numPeers) { // workaround the case when finishedPeers = numPeers, but receiver does
												// not yet complete receiving some Records on fly
			try {
				Thread.sleep(100);	// hope that after a while some records appear in the localCache
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		rec = localCache.poll();
		return rec;
	}

	@Override
	public void close() {
		try {
			t1.join();
			receiveServer.stopServer();
			receiveServer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		child.close();
		System.out.println("Receive " + nodeId + " close");
		// reverse what was done in open()
	}

}
