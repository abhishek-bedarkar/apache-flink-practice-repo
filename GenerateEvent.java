package eventGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;

public class GenerateEvent
{
	public static void main(String[] args) throws IOException
	{
		ServerSocket listener = new ServerSocket(9091);
		try{
				Socket socket = listener.accept();
				System.out.println("New connection: " + socket.toString());
				
				BufferedReader br = new BufferedReader(new FileReader("/home/abhi/Downloads/flink-1.17.1/apps/sales_data.txt"));
				
				try {
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					String line;
					while ((line = br.readLine()) != null){
						System.out.println(line);
						out.println(line);
						Thread.sleep(1000); // wait for 1 min
						System.out.println("----------------------------------------------------------------");
					}
					
				} finally{
					socket.close();
				}
			
		} catch(Exception e ){
			e.printStackTrace();
		} finally{
			
			listener.close();
		}
	}
}
