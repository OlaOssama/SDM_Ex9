package de.tuda.sdm.dmdb.sql.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import de.tuda.sdm.dmdb.net.TCPClient;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.EnumSQLType;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;

/**
 * Base class for implementation of send operator
 * @author melhindi
 *
 */
public abstract class SendBase extends UnaryOperator {

	protected int nodeId; // own nodeId to identify which records to keep locally
	protected Map<Integer,String> nodeMap; // map containing connection information to establish connection to other peers
	protected Map<Integer, TCPClient> connectionMap; // map to store connection to other peers
	protected int partitionColumn; // indicates column number used for repartitioning
	protected int hashFunction = 1; // modulo-value used in hash-function for repartitioning, in general nodeMap.size()

	/**
	 * Constructor of SendBase
	 * @param child - Child operator used to process next calls, e.g., TableScan or Selection
	 * @param nodeId - Own nodeId to identify which records to keep locally
	 * @param nodeMap - Map containing connection information (as "IP:port" or "domain-name:port") to establish connection to other peers
	 * @param partitionColumn - Number of column that should be used to repartion the data
	 */
	public SendBase(Operator child, int nodeId, Map<Integer,String> nodeMap, int partitionColumn) {
		super(child);
		this.nodeId = nodeId;
		this.nodeMap = nodeMap;
		this.hashFunction = nodeMap.size(); // we assume an even repartitioning based on number of nodes
		this.connectionMap = new HashMap<Integer, TCPClient>();
		this.partitionColumn = partitionColumn;
	}
	
	/**
	 * Computes hash of record for re-partitioning / performs repartitioning. ONLY SQLInteger values are supported as repartitioning columns
	 * I.e., determines nodeId to which a record needs to be send
	 * @param record - The record to which to determine the destination node for
	 * @param attColumn - Column number used for hashing/repartitioning. ONLY SQLInteger columns are supported
	 * @return Returns nodeId to which send record a record
	 * 
	 * @throws IllegalArgumentException - Thrown when column is not of txype SQLInteger
	 */
	protected int getNodeIdForRecord(AbstractRecord record, int attColumn) throws IllegalArgumentException{
		int hash = 0;
		if (record.getValue(attColumn).getType() !=  EnumSQLType.SqlInteger) {
			throw new IllegalArgumentException("SendBase - getNodeIdForRecord: Operator only supports shuffeling based on a SQLInteger value");
		};
		if (attColumn >= record.getValues().length) {
			throw new IllegalArgumentException("SendBase - getNodeIdForRecord: Speficied column number to high");
		}

		int value = ((SQLInteger) record.getValue(attColumn)).getValue();

		hash = (value % this.hashFunction);

		return hash;
	}

	/**
	 * Adequately close all connection that where established to peers during init phase
	 */
	protected void closeConnectionsToPeers() {
		Iterator<Integer> it = this.connectionMap.keySet().iterator();
		while(it.hasNext()) {
			TCPClient connection = this.connectionMap.get(it.next());
			if (connection != null) {
				connection.close();
			}else {
				System.out.println("SendBase: WARNING: No connection to close for peer " + this.nodeMap.get(nodeId));
			}
			it.remove(); // avoids a ConcurrentModificationException
		}
	}

	/**
	 * Getter method for hashFunction
	 * @return Returns the hashFunction (i.e. modulo value) used for repartitioning
	 */
	public int getHashFunction() {
		return hashFunction;
	}

	/**
	 * Setter method for hashFunction
	 * @param hashFunction - New hash function (i.e. modulo value) to use for repartitioning
	 */
	public void setHashFunction(int hashFunction) {
		this.hashFunction = hashFunction;
	}

}
