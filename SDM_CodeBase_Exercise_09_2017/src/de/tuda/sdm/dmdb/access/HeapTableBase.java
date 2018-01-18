package de.tuda.sdm.dmdb.access;

import java.util.HashMap;
import java.util.Vector;

import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.PageManager;

public abstract class HeapTableBase extends AbstractTable {
	
	//metadata 
	Vector<Integer> pageNumbers = new Vector<Integer>();
	
	//data
	protected AbstractPage lastPage;
	protected HashMap<Integer, AbstractPage> pages = new HashMap<Integer, AbstractPage>();  //page number->page
		
	/**
	 * Iterator for a table
	 * @author cbinnig
	 *
	 */
	protected class Iterator implements java.util.Iterator<AbstractRecord> {
		private int pagePos;
		private int pageNumber;
		private int slotNumber;
		private AbstractPage currentPage;
		
		Iterator(){
			this.pagePos=0;
			this.pageNumber = HeapTableBase.this.pageNumbers.get(this.pagePos);
			this.slotNumber=0;
			this.currentPage = HeapTableBase.this.getPage(this.pageNumber);
		}
		
		@Override
		public boolean hasNext() {
			
			if(pagePos>=HeapTableBase.this.pageNumbers.size())
				return false;
			
			if(slotNumber>=this.currentPage.getNumRecords() && (pagePos+1)>=HeapTableBase.this.pageNumbers.size())
				return false;
			
			return true;
		}
		
		@Override
		public AbstractRecord next() {
			if(slotNumber>=this.currentPage.getNumRecords()){
				this.pagePos++;
				this.pageNumber = HeapTableBase.this.pageNumbers.get(this.pagePos);
				this.slotNumber=0;
				this.currentPage = HeapTableBase.this.pages.get(pageNumber);
				slotNumber = 0;
			}
			
			AbstractRecord record = HeapTableBase.this.prototype.clone();
			this.currentPage.read(slotNumber++, record);
			return record;
		}
		
		@Override
		public void remove() {
			//Nothing to do at the moment
		}
	}
	
	/**
	 * 
	 * Constructs table from record prototype
	 * @param prototypeRecord
	 */
	public HeapTableBase(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
		
		//create first empty page
		this.lastPage = PageManager.createDefaultPage(this.prototype.getFixedLength());
		this.addPage(lastPage);
	}
	
	/**
	 * Creates an iterator
	 * @return Java iterator
	 */
	public java.util.Iterator<AbstractRecord> iterator(){
		return new Iterator();
	}
	
	/**
	 * Adds a new page to container and creates unique page number
	 * @param page Page to ba added to container
	 */
	protected void addPage(AbstractPage page){
		Integer pageNumber = page.getPageNumber();
		this.pageNumbers.add(pageNumber);
		this.pages.put(pageNumber, page);
	}
	
	/**
	 * Returns page by page number
	 * @param pageNumber Page number
	 * @return page for a given pagenumber
	 */
	protected AbstractPage getPage(int pageNumber){
		return this.pages.get(pageNumber);
	}
	
	
	@Override
	public AbstractRecord lookup(RecordIdentifier rid) {
		return this.lookup(rid.getPageNumber(), rid.getSlotNumber());
	}
}
