package de.tuda.sdm.dmdb.sql.operator;

import java.util.HashMap;

import de.tuda.sdm.dmdb.sql.operator.BinaryOperator;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

public abstract class HashJoinBase extends BinaryOperator {
	protected int leftAtt=0;
	protected int rightAtt=0;
	protected HashMap<AbstractSQLValue, AbstractRecord> hashMap;
	
	protected AbstractRecord leftRecord = null;
	
	public HashJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild);
		this.leftAtt = leftAtt;
		this.rightAtt = rightAtt;
	}
	
}
