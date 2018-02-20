package metaTeam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("serial")
public class Page implements Serializable{

	private String pageName;
	private Entry[] data;
	
	//Constructor for the Page which initializes all attributes
	public Page(String pageName, int keyIndex) throws IOException{
		FileReader fr = new FileReader("./config/DBApp.properties");
		BufferedReader br = new BufferedReader(fr);
		String curr = br.readLine();
		String[] h = curr.split("= ");
		int curr0 = Integer.parseInt(h[1]);
		data = new Entry[curr0];
		this.pageName = pageName;
		br.close();
	}
	
	//helper Method for inserting Entries into a page
	//**Unfinished**
	public void insert(Entry entry, int entryCount) {
		data[entryCount] = entry;
		updatePage();
	}
	
	//helper Method for updating the Serialized Page file
	public void updatePage() {
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(new File("./Data/" + pageName + ".ser"));
			out = new ObjectOutputStream(fos);
			out.writeObject(this);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public Entry[] getData() {
		return this.data;
	}
	
}
