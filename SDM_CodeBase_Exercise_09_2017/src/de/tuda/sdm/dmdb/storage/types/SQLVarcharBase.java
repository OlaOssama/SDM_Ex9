package de.tuda.sdm.dmdb.storage.types;

/**
 * SQL varchar value
 * @author cbinnig
 *
 */
public abstract class SQLVarcharBase extends AbstractSQLValue {

	private static final long serialVersionUID = 1L;

	public static int LENGTH = 8; //fixed length
	
	protected byte[] data; //cache for byte representation
	protected String value; //String value
	
	/**
	 * Constructor with default value and max. length 
	 * @param maxLength
	 */
	public SQLVarcharBase(int maxLength){
		super(EnumSQLType.SqlVarchar, maxLength, false);
		this.value = "";
		this.data = this.serialize();
	}
	
	/**
	 * Constructor with string value and max. length 
	 * @param value
	 * @param maxLength
	 */
	public SQLVarcharBase(String value, int maxLength){
		super(EnumSQLType.SqlVarchar, maxLength, false);
		this.value = value;
		this.data = this.serialize();
	}
	
	/**
	 * Get string value of SQLVarchar
	 * @return
	 */
	public String getValue() {
		return value;
	}
	
	/**
	 * Set string value of SQLVarchar
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
		this.data = this.serialize();
	}
	
	@Override
	public int getFixedLength() {
		return LENGTH;
	}
	
	@Override
	public int getVariableLength() {
		return this.data.length;
	}
	
	@Override
	public String toString(){
		return this.value;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj==null) return false;
		if (!(obj instanceof SQLVarcharBase))
			return false;	
		if (obj == this)
			return true;
		
		SQLVarcharBase cmp = (SQLVarcharBase)obj;
		return this.getMaxLength() == cmp.getMaxLength() && this.value.equals(cmp.value);
	}
	
	@Override
	public int compareTo(AbstractSQLValue o) {
		SQLVarcharBase cmp = (SQLVarcharBase)o;
		return this.value.compareTo(cmp.value);
	}
	
	@Override
	public int hashCode() {
		return this.value.hashCode();
	}

	@Override
	public void parseValue(String data) {
		this.value = data;
	}
}
