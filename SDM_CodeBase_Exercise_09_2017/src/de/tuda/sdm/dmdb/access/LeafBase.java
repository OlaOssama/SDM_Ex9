package de.tuda.sdm.dmdb.access;

import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.PageManager;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 * @author cbinnig
 */
public abstract class LeafBase<T extends AbstractSQLValue> extends AbstractIndexElement<T>{
	/**
	 * 
	 */

	/**
	 * Leaf constructor
	 * @param uniqueBPlusTree TODO
	 */
	public LeafBase(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
		this.indexPage = PageManager.createDefaultPage(uniqueBPlusTree.leafRecPrototype.getFixedLength());
		this.uniqueBPlusTree.addIndexElement(this);
	}
	
	@Override
	public void split(AbstractIndexElement<T> leaf1, AbstractIndexElement<T> leaf2) {
		int cnt = this.indexPage.getNumRecords();
		AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
		int slotNumber=0;
		
		for(; slotNumber<cnt/2; ++slotNumber){
			this.indexPage.read(slotNumber, leafRecord);
			leaf1.indexPage.insert(leafRecord);
		}
		
		for(; slotNumber<cnt; ++slotNumber){
			this.indexPage.read(slotNumber, leafRecord);
			leaf2.indexPage.insert(leafRecord);
		}
		this.uniqueBPlusTree.releaseIndexElement(this.indexPage.getPageNumber());
	}
	
	@Override
	public int binarySearch(T key){
		AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
		return this.binarySearch(key, leafRecord);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T getMaxKey() {
		AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
		this.indexPage.read(this.indexPage.getNumRecords()-1, leafRecord);
		return (T) leafRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
	}
	
	@Override
	protected void print(int level) {
		String indent = "";
		for(int i=0; i<level; ++i){
			indent+="\t";
		}
		
		AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
		
		System.out.println(indent+"Leaf:"+this.indexPage.getPageNumber());
		for(int i=0; i<this.indexPage.getNumRecords();++i){
			this.indexPage.read(i, leafRecord);
			System.out.println(indent + leafRecord.toString());
			
			SQLInteger pageNumber = (SQLInteger)leafRecord.getValue(UniqueBPlusTreeBase.PAGE_POS);
			SQLInteger slotNumber = (SQLInteger)leafRecord.getValue(UniqueBPlusTreeBase.SLOT_POS);
			System.out.println(indent + this.uniqueBPlusTree.table.lookup(pageNumber.getValue(), slotNumber.getValue()));
		}
		System.out.println("");
	}
}