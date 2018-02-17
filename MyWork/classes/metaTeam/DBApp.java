package metaTeam;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {

	private static String[] acceptedTypes = {"java.lang.Integer","java.lang.String", "java.lang.Double", "java.lang.Boolean", "java.util.Date"};

	public static void init() {
		File file = new File("MetaData.csv");
	}

	public static void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws DBAppException, IOException {
		FileWriter fWriter = new FileWriter("./MetaData.csv" , true);
		BufferedWriter bw = new BufferedWriter(fWriter);
//		try {
//		for(Enumeration<String> e = colNameType.keys();e.hasMoreElements();) {
//			String colName = e.nextElement();
//			String colType = colNameType.get(colName);
//			for (String type : acceptedTypes) {
//				if(colType.equals(type)) {
//					break;
//				}
//				else if(type.equals("Java.util.Date")) {
//					throw (DBAppException d);
//				}
//			}
//		}
//		}
//		catch(DBAppException) {
//			System.out.println("Wrong type");
//		}
		String toBeAdded = "";
		for(Enumeration<String> e = getKeys(colNameType); e.hasMoreElements();) {
			String colName = e.nextElement();
			String colType = colNameType.get(colName);


			toBeAdded = tableName + "," + colName + "," + colType + ",";
			if(colName.equals(clusteringKey)) {
				toBeAdded += "True,False";
			}
			else {
				toBeAdded += "False,False";
			}
			bw.append(toBeAdded + "\n");
		}
		bw.close();
		new Table(clusteringKey, tableName, colNameType);
	}

	public void createBRINIndex(String tableName, String colName) throws DBAppException {

	}

	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		Enumeration<String> e = colNameValue.keys();
		while(e.hasMoreElements())
		{	
		Entry entry1 = new Entry(colNameValue, tableName);
		
		}
		
	}

	public void updateTable(String tableName, String key, Hashtable<String,Object> colNameValue) throws DBAppException {

	}

	public void deleteFromTable(String tableName, Hashtable<String,Object> colNameType) throws DBAppException {
		FileInputStream fis = null;
        ObjectInputStream in = null;
        Table t;
        try {
            fis = new FileInputStream(tableName);
            in = new ObjectInputStream(fis);
            t = (Table) in.readObject();
            in.close();

    		for (int i=0; i<=t.getPages().size(); i++) {
    			 for (int j=0; j<=t.getPages().get(i).getData().length; j++) {
    				 for (int k=0; k<t.getPages().get(i).getData()[j].getRow().size(); k++)
    				  if (t.getPages().get(i).getData()[j].getRow().get(k).equals(colNameType))
    					  t.getPages().get(i).getData()[j].getRow().remove(k);   }}}
	catch (Exception ex) {
            ex.printStackTrace();
        }
	}

	public void selectFromTable(String tableName, String columnName, Object[] objarrValues, String[] strarrOperators) throws DBAppException {

	}

	public static void main(String[] args) throws IOException, DBAppException {
		init();
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		createTable( strTableName, "id", htblColNameType );
	}

	public static Enumeration<String> getKeys(Hashtable<String,String> in){
		return in.keys();
	}

}
