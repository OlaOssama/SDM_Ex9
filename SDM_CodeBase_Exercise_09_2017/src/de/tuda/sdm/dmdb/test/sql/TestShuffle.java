package de.tuda.sdm.dmdb.test.sql;

import org.junit.Assert;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestShuffle extends TestCase{

	public void testShuffleSimple1() throws InterruptedException {

		AbstractRecord templateRecord = new Record(1);
		templateRecord.setValue(0, new SQLInteger(0));

		HeapTable htable1 = new HeapTable(templateRecord);
		HeapTable htable2 = new HeapTable(templateRecord);

		Queue<AbstractRecord> resultList =  new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> expectedResult =  new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> expectedResult2 =  new LinkedList<AbstractRecord>();

		int numRecords = 200;
		for (int i = 0; i < numRecords; i++) {

			AbstractRecord recrod = new Record(1);
			recrod.setValue(0, new SQLInteger(i));

			if (i < numRecords/2) {
				htable1.insert(recrod);
			}else {
				htable2.insert(recrod);
			}

			int hashValue = i % 2;
			if (hashValue == 0) {
				expectedResult.add(recrod);
			}
			else {
				expectedResult2.add(recrod);
			}

		}

		int nodeId = 0;
		int port = 8000;
		Map<Integer, String> nodeMap = new HashMap<Integer, String>();
		nodeMap.put(nodeId, "localhost:"+port);
		nodeMap.put(nodeId+1, "localhost:"+(port+1));
		int partitionColumn = 0;



		Runnable task1 = () -> {
			TableScan tableScan = new TableScan(htable1);
			// receive operator listening on port 8000
			Shuffle shuffleOperator = new Shuffle(tableScan, nodeId, nodeMap, port, partitionColumn);
			shuffleOperator.open();
			AbstractRecord next;
			while ((next = shuffleOperator.next()) != null) {
				resultList.offer(next);
			}
			shuffleOperator.close();
		};

		Runnable task2 = () -> {
			TableScan tableScan = new TableScan(htable2);
			// receive operator listening on port 8001
			Shuffle shuffleOperator = new Shuffle(tableScan, nodeId+1, nodeMap, port+1, partitionColumn);
			shuffleOperator.open();
			AbstractRecord next;
			while ((next = shuffleOperator.next()) != null) {
				resultList2.offer(next);
			}
			shuffleOperator.close();
		};

		Thread peer1 = new Thread(task1);
		Thread peer2 = new Thread(task2);
		peer1.start(); peer2.start();
		peer1.join(); peer2.join();

		System.out.println("Record list1: " + resultList);
		System.out.println("Record list2: " + resultList2);
		Assert.assertTrue(resultList.size() == numRecords / 2);
		Assert.assertTrue(resultList2.size() == numRecords / 2);
		for (AbstractRecord abstractRecord : resultList) {
			Assert.assertTrue(expectedResult.contains(abstractRecord));
		}
		for (AbstractRecord abstractRecord : resultList2) {
			Assert.assertTrue(expectedResult2.contains(abstractRecord));
		}

	}
}
