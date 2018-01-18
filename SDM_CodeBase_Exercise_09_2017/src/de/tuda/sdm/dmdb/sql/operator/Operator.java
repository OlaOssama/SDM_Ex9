package de.tuda.sdm.dmdb.sql.operator;

import de.tuda.sdm.dmdb.storage.AbstractRecord;

public abstract class Operator {
	public abstract void open();
	
	public abstract AbstractRecord next();
	
	public abstract void close();
}
