package de.tuda.sdm.dmdb.test.sql;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.exercise.HashJoin;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestHashJoin extends TestCase{

	public void testHashJoinNext() {

		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();

		AbstractRecord templateRecord = new Record(2);
		templateRecord.setValue(0, new SQLInteger(0));
		templateRecord.setValue(1, new SQLInteger(0));

		HeapTable htLeft = new HeapTable(templateRecord);
		HeapTable htRight = new HeapTable(templateRecord);

		int numRecords = 100;
		for (int i = 0; i < numRecords; i++) {
			AbstractRecord record = new Record(2);
			record.setValue(0, new SQLInteger(i));
			record.setValue(1, new SQLInteger(i));

			htLeft.insert(record);
			htRight.insert(record);

			expectedResult.add(record.append(record));
		}

		TableScan tsLeft = new TableScan(htLeft);
		TableScan tsRight = new TableScan(htRight);

		HashJoin join = new HashJoin(tsLeft, tsRight, 1, 0);

		join.open();
		for (AbstractRecord abstractRecord : expectedResult) {
			AbstractRecord next = join.next();
			System.out.println(next);
			Assert.assertEquals(abstractRecord, next);
		}
	}

}
