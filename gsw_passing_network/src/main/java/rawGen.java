//generate raw.csv for gsw network graph

//import dependencies
import java.io.*;

public class Sandbox {

	public static void main(String[] args) {
		try {
			//define file readers and writers
			File file = new File("passes.csv");
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			PrintWriter pw = new PrintWriter(new FileWriter("out.txt"));

			//extract header for csv file
			String header = br.readLine();
			System.out.println(header);
			pw.write(header);
			pw.write("\n");

			//iterate through each line to generate data
			String line;
			while ((line = br.readLine()) != null) {
				String[] word = line.split(",");
				int passes = Integer.parseInt(word[4]);
				for(int i = 0; i < passes; i++) {
					System.out.println(line);
					pw.write(line);
					pw.write("\n");
				}
			}
			//close readers and writers
			fr.close();
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
