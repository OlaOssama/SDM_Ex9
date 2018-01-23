package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuda.sdm.dmdb.net.TCPClient;
import de.tuda.sdm.dmdb.net.TCPServer;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SendBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;

/**
 * Implementation of send operator
 * 
 * @author melhindi
 *
 */
public class Send extends SendBase {

	/**
	 * Constructor of Send
	 * 
	 * @param child
	 *            - Child operator used to process next calls, e.g., TableScan or
	 *            Selection
	 * @param nodeId
	 *            - Own nodeId to identify which records to keep locally
	 * @param nodeMap
	 *            - Map containing connection information (as "IP:port" or
	 *            "domain-name:port") to establish connection to other peers
	 * @param partitionColumn
	 *            - Number of column that should be used to repartition the data
	 */
	public Send(Operator child, int nodeId, Map<Integer, String> nodeMap, int partitionColumn) {
		super(child, nodeId, nodeMap, partitionColumn);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// init child
		child.open();
		// create a client socket for all peer nodes using information in nodeMap
		// store client socket in map for later use

		connectionMap = new HashMap<Integer, TCPClient>();

		for (Map.Entry<Integer, String> entry : nodeMap.entrySet()) {
			if (entry.getKey() != nodeId) {
				String address = nodeMap.get(nodeId);
				String hostname = address.substring(0, address.indexOf(":"));
				int port = Integer.valueOf(address.substring(address.indexOf(":")));

				try {
					connectionMap.put(entry.getKey(), new TCPClient(hostname, port));

				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// retrieve next from child and determine whether to keep record local or send
		// to peer

		// reached end, close connections to peers
		AbstractRecord rec;
		Queue<AbstractRecord> resultList =  new LinkedList<AbstractRecord>();
		
		do {
			rec = child.next();
			if (rec != null) {
				int id = ((SQLInteger) rec.getValue(partitionColumn)).getValue() % hashFunction; // nodeId of peer to send
				if (id == (nodeId % hashFunction)) { // store locally
					resultList.add(rec);
				} else { // send to a peer
//					TCPClient peer = connectionMap.get(id);
					connectionMap.get(id).sendRecord(rec);
				}
			} else {
				for (Map.Entry<Integer, TCPClient> entry : connectionMap.entrySet()) {
					entry.getValue().close();
				}
			}
		} while (rec != null);
		return resultList.remove();
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open() - hint there is a helper method that you can
		// use
		child.close();
	}

}
