package de.tuda.sdm.dmdb.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Handler class used in TCPServer to process in-comming client connections
 * @author melhindi
 *
 */
/**
 * @author melhindi
 *
 */
public class ReceiveHandler {

	/**
	 * Wraps the runTask() method to create an additional layer of abstractions
	 * Monitors client connections and counts closing sockets (peers finished sending)
	 * Other Handler implementations can override runTask() to define a different handling behaviour
	 * 
	 * @param socket - Socket of the client connection passed in by the listener
	 * @param pool - Thread to submit handling task to
	 * @param localCache - Receive Cache to pass to handler for storing incoming records
	 * @param finishedPeers - Reference to the counter for the registration of peers that finished sending data
	 */
	public void handle(final Socket socket, ExecutorService pool, Queue<AbstractRecord> localCache, AtomicInteger finishedPeers) {
		pool.execute(new Runnable() {
			@Override
			public void run() {
				SocketAddress remoteSocketAddress = socket.getRemoteSocketAddress();
				SocketAddress localSocketAddress = socket.getLocalSocketAddress();
				System.out.println("Handler " + localSocketAddress + ": Connection to " + remoteSocketAddress + " open");
				runTask(socket, localCache);
				try {
					if (socket != null)
						socket.close();
				} catch (IOException e) {
					System.err.println("Handler " + localSocketAddress + ": Unable to close connection to " + remoteSocketAddress);
					e.printStackTrace();
				}
				System.out.println("Handler " + localSocketAddress + ": Connection to " + remoteSocketAddress + " closed");
				// handler finished processing and connection closed
				// increase counter of finished peers
				finishedPeers.addAndGet(1);
				System.out.println("Handler " + localSocketAddress + ": Num finished peers = "+finishedPeers.get());
			}
		});
	}

	/**
	 * Method that defines how in-coming connections are processed
	 * @param socket - Socket of client connection 
	 * @param localCache - Reference to the local cache used to store incoming records from the client connection
	 */
	public void runTask(Socket socket, Queue<AbstractRecord> localCache) {
		SocketAddress localSocketAddress = socket.getLocalSocketAddress();
		try (
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				) {
			System.out.println("Handler " + localSocketAddress + ": Created Object Output Stream");
			AbstractRecord input;
			// check if the local cache implementation used is thread-safe
			boolean lock = !(localCache.getClass().getPackage().getName().equals("java.util.concurrent"));
			boolean result = false;
			while ((input = (AbstractRecord) in.readObject()) != null) {
//				System.out.println("Handler " + localSocketAddress + ": Received new Object: " + input);

				if (lock) {
					// local cache not thread-safe, lock it
					synchronized (localCache) {
						result = localCache.offer(input);
					}
				}else {
					result = localCache.offer(input);
				}

				if (!result) {
					System.err.println("Handler " + localSocketAddress + ": Could not add object to localCache!");
				}else {
//					System.out.println("Handler " + localSocketAddress + ": Added object to local cache");
				}
			}

		} catch (EOFException e) {
			System.out.println("Handler " + localSocketAddress + ": Got a EOFException. This indicates that a client closed the connection");
		}
		catch (IOException e) {
			System.err.println("Handler " + localSocketAddress + ": Got the following error:");
			e.printStackTrace();

		} catch (ClassNotFoundException e) {
			System.err.println("Handler " + localSocketAddress + ": Got the following error:");
			e.printStackTrace();
		} 

	}
}