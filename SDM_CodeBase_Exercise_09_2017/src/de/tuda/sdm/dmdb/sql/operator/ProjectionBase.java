package de.tuda.sdm.dmdb.sql.operator;

import java.util.Vector;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public abstract class ProjectionBase extends UnaryOperator {
	protected Vector<Integer> attributes;
	
	public ProjectionBase(Operator child, Vector<Integer> attributes) {
		super(child);
		
		this.attributes = attributes;
	}
}
