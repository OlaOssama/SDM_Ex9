package de.tuda.sdm.dmdb.access;

import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Abstract class for an single dimensional unique index
 * @author cbinnig
 *
 * @param <T> type of key
 */
public abstract class AbstractUniqueIndex<T extends AbstractSQLValue>{

	protected AbstractTable table; //table to be indexed
	
	/**
	 * Creates an index for a given table
	 * @param table
	 */
	public AbstractUniqueIndex(AbstractTable table){
		this.table = table;
	}
	
	
	/**
	 * Returns table
	 * @return
	 */
	public AbstractTable getTable() {
		return table;
	}

	/**
	 * Inserts a record into the index and into its table
	 * @param record
	 */
	public abstract boolean insert(AbstractRecord record);

	/**
	 * Returns a record for a given key
	 * @param key
	 * @return
	 */
	public abstract AbstractRecord lookup(T key);
	
	
	/**
	 * Print the index for debugging
	 */
	public abstract void print();
	 
}
