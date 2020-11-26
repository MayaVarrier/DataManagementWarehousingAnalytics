import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//import sun.jvm.hotspot.runtime.Threads;

public class concurrentTransaction {
	
	public static void main(String[] args) {
		Transaction T1 = new Transaction();
		Transaction T2 = new Transaction();
		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
					//T.createConnection();
					Connection con = T1.createConnection();
					T1.firstTransaction(con);
					}			
		}, "T1 City");
		
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				//T.createConnection();
				Connection con = T2.createConnection();
				T2.secondTransaction(con);
			}
		}, "T2 City");
		
		t1.setPriority(10);
		t2.setPriority(1);
		t1.start();
		t2.start();
		
		T1.closeConnection();
		T2.closeConnection();
	}

}	
 