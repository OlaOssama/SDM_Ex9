package de.tuda.sdm.dmdb.test;

import de.tuda.sdm.dmdb.catalog.CatalogManager;

public class TestCase extends junit.framework.TestCase {

	public TestCase() {
	}

	public TestCase(String name) {
		super(name);
	}

	public void setUp(){	
		CatalogManager.clear();
	}
}
