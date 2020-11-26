
public class distributedTransaction {
	
	
	public static void main(String args[]) {
		
		Transaction T1 = new Transaction();
		Transaction T2 = new Transaction();
		
		try {
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
					T1.createConnection();
					T1.firstDistributedTransaction();
					}			
		}, "Trans1");
		
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				T2.createConnection();
				T2.secondDistributedTransaction();
			}
		}, "Trans2");
		
		t1.setPriority(10);
		t2.setPriority(1);
		t1.start();
		t2.start();
		}
		catch(Exception e) {
			
		}
		
		finally {
		T1.closeConnection();
		T2.closeConnection();
		}
		
	}

}
