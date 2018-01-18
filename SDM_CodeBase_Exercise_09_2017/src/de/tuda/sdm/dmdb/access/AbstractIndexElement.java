package de.tuda.sdm.dmdb.access;

import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Abstract class for an index element (node or leaf)
 * @author cbinnig
 */
public abstract class AbstractIndexElement<T extends AbstractSQLValue>{
	/**
	 * 
	 */
	protected UniqueBPlusTreeBase<T> uniqueBPlusTree;

	/**
	 * @param uniqueBPlusTree
	 */
	AbstractIndexElement(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
		this.uniqueBPlusTree = uniqueBPlusTree;
	}

	//index page to store index entries
	protected AbstractPage indexPage;
	
	/**
	 * Checks if index page exceeds max. fill grade
	 * @return
	 */
	public boolean isFull(){
		return (this.indexPage.getNumRecords()>uniqueBPlusTree.maxFillGrade);
	}
	
	/**
	 * creates a leaf or a node depending on type of object
	 * @return
	 */
	public abstract AbstractIndexElement<T> createInstance();
	
	/**
	 * lookup a record for a key in index 
	 * @param key
	 * @return
	 */
	public abstract AbstractRecord lookup(T key);
	
	/**
	 * Inserts a new record into index
	 * @param record
	 * @return True is record could be inserted, false if key is already used
	 */
	public abstract boolean insert( T key, AbstractRecord record);
	
	/**
	 * Get maximum key value of index element
	 * @return
	 */
	public abstract T getMaxKey();
	
	/**
	 * Split index element into two elements
	 * @param element1 Reference to first element 
	 * @param element2 Reference to second element
	 */
	public abstract void split(AbstractIndexElement<T> element1, AbstractIndexElement<T> element2);
	
	/**
	 * Binary search for key
	 * @param key
	 * @return Slot number of node entry
	 */
	public abstract int binarySearch(T key);
	
	/**
	 * Print index elements  
	 * @param level
	 */
	protected abstract void print(int level);
	
	
	/**
	 * Print index elements
	 * @param level
	 */
	public void print() {
		this.print(0);
	}
	
	/**
	 * Returns page number of index element
	 * @return
	 */
	public int getPageNumber(){
		return this.indexPage.getPageNumber();
	}
	
	/**
	 * Binary search on index element. If element is not found then 
	 * it returns pointer to next bigger element in index element
	 * or points to slot after last element if key is bigger
	 * than all index element entries.
	 * 
	 * @param key lookup key
	 * @param indexRecord record to read from index element (node or leaf)
	 * @return slot number of node entry
	 */
	protected int binarySearch(T key, AbstractRecord indexRecord){
		int end = this.indexPage.getNumRecords() - 1;
		int start = 0;
		int center = -1;
		
		while(start<=end){
			center = (start + end) / 2;
			this.indexPage.read(center, indexRecord);
			AbstractSQLValue keyValue = indexRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
			
			if(key.compareTo(keyValue)==0){
				return center;
			}
			else if(key.compareTo(keyValue)<0){
				end = center - 1;
			}
			else if(key.compareTo(keyValue)>0){
				start = center + 1;
			}
		}
		return start;
	}

	public UniqueBPlusTreeBase<T> getUniqueBPlusTree() {
		return uniqueBPlusTree;
	}

	public void setUniqueBPlusTree(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
		this.uniqueBPlusTree = uniqueBPlusTree;
	}

	public AbstractPage getIndexPage() {
		return indexPage;
	}

	public void setIndexPage(AbstractPage indexPage) {
		this.indexPage = indexPage;
	}
}