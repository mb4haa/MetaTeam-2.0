package metaTeam;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

public class DBApp {

	private static String[] acceptedTypes = {"java.lang.Integer","java.lang.String", "java.lang.Double", "java.lang.Boolean", "java.util.Date"};

	public static void init() {
	}

	public static void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType) throws DBAppException, IOException {
		FileWriter fWriter = new FileWriter("./MetaData.csv" , true);
		BufferedWriter bw = new BufferedWriter(fWriter);
		String toBeAdded = "";
		for(Enumeration<String> e = getKeys(colNameType);e.hasMoreElements();) {
			String type = colNameType.get(e.nextElement());
			if(! checkType(type)) {
				System.out.println("Invalid Type:" + type);
				return;
			}
		}
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

	public static void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException {
		for(Enumeration<String> e = colNameValue.keys();e.hasMoreElements();) {
			String curr = e.nextElement();
			if(! checkInputType(tableName, curr, colNameValue.get(curr))) {
				System.out.println("Wrong Input type for " + curr);
				return;
			}
		}
		FileInputStream fis = null;
		ObjectInputStream in = null;
		fis = new FileInputStream("./Data/" + tableName + ".ser");
		in = new ObjectInputStream(fis);
		Table t = (Table) in.readObject();
		in.close();
		Entry entry = new Entry(colNameValue, tableName);
		t.insert(tableName, entry);
	}

	public void updateTable(String tableName, String key, Hashtable<String,Object> colNameValue) throws DBAppException {

	}

	public void deleteFromTable(String tableName, Hashtable<String,Object> colNameType) throws DBAppException, IOException {
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
	

	public static Enumeration<String> getKeys(Hashtable<String,String> in){
		return in.keys();
	}

	public static String[] getAcceptedTypes() {
		return acceptedTypes;
	}

	public static void setAcceptedTypes(String[] acceptedTypes) {
		DBApp.acceptedTypes = acceptedTypes;
	}

	public static boolean checkType(String type) {
		boolean ret = false;
		for (String acc : acceptedTypes) {
			if(acc.equals(type)) {
				ret = true;
			}
		}
		return ret;
	}
	
	public static boolean checkInputType(String tableName, String colName, Object o) throws IOException {
		boolean ret = false;
		List<String> list=new ArrayList<>();
		FileReader fr = new FileReader("./MetaData.csv");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		while(curr != null) {
			if(curr.contains("" + tableName + "," + colName)) {
				String type = o.getClass().getName();
				if(curr.contains(type)) {
					ret = true;
				}
			}
			curr = br.readLine();
		}
		return ret;
	}

	public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		createTable( strTableName, "id", htblColNameType );

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double(0.95));
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		insertIntoTable( strTableName , htblColNameValue );
	}
	
}
