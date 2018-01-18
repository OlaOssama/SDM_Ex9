package de.tuda.sdm.dmdb.sql.operator;

import java.util.Map;

import de.tuda.sdm.dmdb.sql.operator.exercise.Receive;
import de.tuda.sdm.dmdb.sql.operator.exercise.Send;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of shuffle operator to repartition data accross multiple nodes
 * @author melhindi
 *
 */
public class Shuffle extends UnaryOperator {

	protected Receive receiveOperator; // required to receive records from peers
	protected Send sendOperator; // required to send records to peers

	/**
	 * Constructor of Shuffle operator
	 * @param child - Child operator in query plan
	 * @param nodeId - Own nodeId used to identify local records in send-operator
	 * @param nodeMap - Map of the form <NodeId:"IP:port"> containing connection information of all peers in the network
	 * @param listenerPort - Port on which receive operator will listen for incoming connections
	 * @param partitionColumn - Column number which will be used by send-operator to repartition the data
	 */
	public Shuffle(Operator child, int nodeId, Map<Integer,String> nodeMap, int listenerPort, int partitionColumn) {
		super(child);
		this.sendOperator = new Send(child, nodeId, nodeMap, partitionColumn);
		this.receiveOperator = new Receive(this.sendOperator, nodeMap.size(), listenerPort, nodeId);
	}

	@Override
	public void open() {
		// inits network and listener thread and handler threads
		this.receiveOperator.open();
	}

	@Override
	public AbstractRecord next() {
		// call next on child
		return this.receiveOperator.next();
	}


	@Override
	public void close() {
		this.receiveOperator.close();
	}

}
