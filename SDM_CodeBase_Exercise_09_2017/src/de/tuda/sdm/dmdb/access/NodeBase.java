package de.tuda.sdm.dmdb.access;

import java.util.Vector;

import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.PageManager;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index node
 * @author cbinnig
 *
 */
public abstract class NodeBase<T extends AbstractSQLValue> extends AbstractIndexElement<T>{

	/**
	 * Node constructor
	 * @param uniqueBPlusTree TODO
	 */
	public NodeBase(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
		this.indexPage = PageManager.createDefaultPage(this.uniqueBPlusTree.nodeRecPrototype.getFixedLength());
		this.uniqueBPlusTree.addIndexElement(this);
	}

	@Override
	public void split(AbstractIndexElement<T> node1, AbstractIndexElement<T> node2) {
		int cnt = this.indexPage.getNumRecords();
		AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
		int slotNumber=0;
		
		for(; slotNumber<cnt/2; ++slotNumber){
			this.indexPage.read(slotNumber, nodeRecord);
			node1.indexPage.insert(nodeRecord);
		}
		
		for(; slotNumber<cnt; ++slotNumber){
			this.indexPage.read(slotNumber, nodeRecord);
			node2.indexPage.insert(nodeRecord);
		}
		this.uniqueBPlusTree.releaseIndexElement(this.indexPage.getPageNumber());
	}
	
	@Override
	public int binarySearch(T key){
		AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
		int index =  this.binarySearch(key, nodeRecord);
		int last = this.indexPage.getNumRecords() - 1;
		return (index<last)?index:last;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public T getMaxKey() {
		AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
		this.indexPage.read(this.indexPage.getNumRecords()-1, nodeRecord);
		return (T) nodeRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
	}

	@Override
	protected void print(int level) {
		String indent = "";
		for(int i=0; i<level; ++i){
			indent+="\t";
		}
		
		Vector<AbstractIndexElement<T>> children = new Vector<AbstractIndexElement<T>>();
		AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
		
		System.out.println(indent+"Node:"+this.indexPage.getPageNumber());
		for(int i=0; i<this.indexPage.getNumRecords();++i){
			this.indexPage.read(i, nodeRecord);
			System.out.println(indent+nodeRecord.toString());
			
			SQLInteger pageNumber = (SQLInteger)nodeRecord.getValue(UniqueBPlusTreeBase.PAGE_POS);
			children.add(this.uniqueBPlusTree.getIndexElement(pageNumber.getValue()));
		}
		System.out.println("");
		
		for(AbstractIndexElement<T> child: children){
			child.print(level+1);
		}
	}
}