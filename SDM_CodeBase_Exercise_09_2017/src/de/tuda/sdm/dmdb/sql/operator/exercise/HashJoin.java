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
		leftChild.open();
		rightChild.open();

		hashMap = new HashMap<AbstractSQLValue, AbstractRecord>();

		// build hashtable based on right-handside table
		AbstractRecord rec;
		do {
			rec = rightChild.next();
			if (rec != null) {
				hashMap.put(rec.getValue(rightAtt), rec);
			}
		} while (rec != null);
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// probe HashTable and return next record
		leftRecord = leftChild.next();
		AbstractRecord rec;
		if (leftRecord != null) {
			rec = hashMap.get(leftRecord.getValue(leftAtt));
			if (rec != null) {
				return leftRecord.append(rec);
			}
		}
		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		leftChild.close();
		rightChild.close();
	}
}
