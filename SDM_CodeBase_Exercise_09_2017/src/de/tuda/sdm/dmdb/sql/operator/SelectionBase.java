package de.tuda.sdm.dmdb.sql.operator;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

@SuppressWarnings("unused")
public abstract class SelectionBase extends UnaryOperator {
	protected int attribute;
	protected AbstractSQLValue constant;
	
	public SelectionBase(Operator child, int attribute, AbstractSQLValue constant) {
		super(child);
		
		this.attribute = attribute;
		this.constant = constant;
	}

}
