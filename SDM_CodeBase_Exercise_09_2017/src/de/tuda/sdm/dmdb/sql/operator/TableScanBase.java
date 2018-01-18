package de.tuda.sdm.dmdb.sql.operator;

import java.util.Iterator;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public abstract class TableScanBase extends Operator {
	protected AbstractTable table;
	protected Iterator<AbstractRecord> tableIter;
	
	public TableScanBase(AbstractTable table){
		super();
		
		this.table = table;
	}

}
