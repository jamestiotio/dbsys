package simpledb.common;

import simpledb.storage.DbFile;

/**
 * The Table class stores all the information of a table.
 * This includes a DbFile file, a String name, and a String pkeyField.
 * This class is used by the Catalog class for a better, neater, and cleaner code organization.
 */
public class Table {
    private DbFile file;
    private String name;
    private String pkeyField;

    public Table(DbFile file, String name, String pkeyField) {
		this.file = file;
		this.name = name;
		this.pkeyField = pkeyField;
	}

	public DbFile getFile() {
		return this.file;
	}

	public String getName() {
		return this.name;
	}

	public String getPkeyField() {
		return this.pkeyField;
	}
}
