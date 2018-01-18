package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.HashMap;

import de.tuda.sdm.dmdb.sql.operator.HashJoinBase;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

public class HashJoin extends HashJoinBase {

	public HashJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	@Override
	public void open() {
		// TODO: implement this method

		// build hashmap

	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// probe HashTable and return next record

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method

	}
}
