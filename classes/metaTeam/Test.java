package metaTeam;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Test {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		FileInputStream fis = null;
		ObjectInputStream in = null;
		for(int i = 0; i < 20; i++) {
			System.out.println("DATA FOR TABLE:" + i);
			fis = new FileInputStream("./Data/Student" + i + ".ser"); //change page
			in = new ObjectInputStream(fis);
			Page p = (Page) in.readObject();

			int count = Table.getEntryCount(p.getData());

			for(int j = 0;j < count; j++) {
				if(p.getData()[j] != null) {
					Entry entry = p.getData()[j];
					System.out.println(entry.getRow().get(0));
				}
				else {
					System.out.println();
				}
			}
		}
	}
}
