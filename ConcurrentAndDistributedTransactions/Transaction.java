import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Transaction {


    Connection localCon = null;
  	Statement localStmt = null;
  	Connection remoteCon = null;
	Statement remoteStmt = null;	  	
  	
	boolean firstReadValue = false;
	boolean secondReadValue = false;
	boolean firstUpdateValue = false;
	boolean secondUpdateValue = false;
	boolean commitValue = false;
	
	boolean geolocationLock = false;
	boolean customerLock = false;
	boolean orderLock = false;
	boolean productLock = false;
	boolean firstLocalCommit = false;
	boolean firstRemoteCommit = false;
	boolean secondLocalCommit = false;
	boolean secondRemoteCommit = false;
	
	public Connection createConnection() {
		
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			
			localCon = DriverManager.getConnection( "jdbc:mysql://localhost:3306/data5408","root","password");
			remoteCon = DriverManager.getConnection( "jdbc:mysql://35.184.43.105:3306/data5408","root","aquarius");
			
			localCon.setAutoCommit(false);
			localCon.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			remoteCon.setAutoCommit(false);
			remoteCon.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			localStmt = localCon.createStatement();  
			remoteStmt = remoteCon.createStatement();
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return localCon;
		
		}
	
	public synchronized void firstTransaction (Connection con){
			
		Statement localStmt = null;
		try {
			localStmt = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
			try {
				
				ResultSet rs =localStmt.executeQuery("select * from customers where customer_zip_code_prefix = 01151");
				//while(rs.next())  
					System.out.println(Thread.currentThread().getName()+"-- select");
					firstReadValue = true;
					
					if(firstReadValue) {
						try {
							wait(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					
				String updateSql = "update customers set customer_city = '"+Thread.currentThread().getName()+"' where customer_zip_code_prefix = 01151 ";
				localStmt.executeUpdate(updateSql);
				
				ResultSet rs1 =localStmt.executeQuery("select * from customers where customer_zip_code_prefix = 1151");
				//while(rs1.next())  
				System.out.println(Thread.currentThread().getName()+"-- update");
				firstUpdateValue = true;
				
				
				if(!firstUpdateValue || !secondUpdateValue) {
				try {
					wait();
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}
				
				if(commitValue)
				{
				con.commit();
				System.out.println(Thread.currentThread());
				}
								
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
	public synchronized void secondTransaction (Connection con){
		
		Statement localStmt = null;
		try {
			localStmt = con.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
			
			try {
				
				ResultSet rs =localStmt.executeQuery("select * from customers where customer_zip_code_prefix = 01151");
				//while(rs.next())  
					System.out.println(Thread.currentThread().getName()+"-- select");
					
					notifyAll();
					
					try {
						wait(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				String updateSql = "update customers set customer_city = '"+Thread.currentThread().getName()+"' where customer_zip_code_prefix = 01151 ";
				localStmt.executeUpdate(updateSql);
				
				//ResultSet rs1 =localStmt.executeQuery("select * from customers where customer_zip_code_prefix = 1151");
				//while(rs1.next())  
				System.out.println(Thread.currentThread().getName()+"-- update");
				secondUpdateValue = true;
				
				if(firstUpdateValue && secondUpdateValue) {
					con.commit();
					commitValue = true;
					notifyAll();
					System.out.println(Thread.currentThread());
				}
									
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
	public synchronized void firstDistributedTransaction() {
		
		int first = 0;
		int second = 0;
		int third = 0;
		int fourth = 0;
		int fifth = 0;
		
		try {
			if(!geolocationLock) {
			geolocationLock = true;
			String firstUpdateSql = "update geolocation set geolocation_city = 'ABC' where geolocation_zip_code_prefix = 4011 ";
			first = localStmt.executeUpdate(firstUpdateSql);
			System.out.println("FU1");
			}
			
			notifyAll();
			if(!geolocationLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!customerLock) {
			customerLock = true;
			String secondUpdateSql = "update customers set customer_city = 'TCR' where customer_zip_code_prefix = 13056 ";
			second = localStmt.executeUpdate(secondUpdateSql);
			System.out.println("FU2");
			}
			
			notifyAll();
			if(!customerLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!orderLock) {
			orderLock = true;
			String thirdUpdateSql = "update orders set order_status = 'pending' where order_id = '949d5b44dbf5de918fe9c16f97b45f8a' ";
			third = remoteStmt.executeUpdate(thirdUpdateSql);
			System.out.println("FU3");
			}
		
			notifyAll();
			if(!orderLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!productLock) {
				productLock = true;
				String fourthUpdateSql = "insert into productCategoryNameTranslation values ('maya' , 'magic')";
				fourth = remoteStmt.executeUpdate(fourthUpdateSql);
				System.out.println("FI4");
				
				String fifthUpdateSql = "insert into productCategoryNameTranslation values ('harsh' , 'happy')";
				fifth = remoteStmt.executeUpdate(fifthUpdateSql);
				System.out.println("FI5");
				
				}
			
			notifyAll();
			if(!productLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(first != 0 && second != 0 && third !=0 && fourth != 0 && fifth != 0)
			{
				try {
					localCon.commit();
					System.out.println("Local");
					firstLocalCommit = true;
					remoteCon.commit();
					firstRemoteCommit = true;
					System.out.println("Remote");
					}
						
					catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				finally {

					if(!firstLocalCommit || !firstRemoteCommit)
					{
							localCon.rollback();
							remoteCon.rollback();
					}
					
					geolocationLock = false;
					customerLock = false;
					orderLock = false;
					productLock = false;
				}
			}
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public synchronized void secondDistributedTransaction() {
		
		
		try {
			int first = 0;
			int second = 0;
			int third = 0;
			int fourth = 0;
			int fifth = 0;
			
			if(!geolocationLock){
			geolocationLock = true;
			String firstUpdateSql = "update geolocation set geolocation_city = 'ABC' where geolocation_zip_code_prefix = 4011 ";
			first = localStmt.executeUpdate(firstUpdateSql);
			//writeLock = false;
			System.out.println("SU1");
			}
			
			notifyAll();
			if(!geolocationLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!customerLock) {
			customerLock = true;
			String secondUpdateSql = "update customers set customer_city = 'TCR' where customer_zip_code_prefix = 13056 ";
			second = localStmt.executeUpdate(secondUpdateSql);
			//writeLock = false;
			System.out.println("SU2");
			}
			
			notifyAll();
			if(!customerLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			
			if(!orderLock) {
			orderLock = true;
			String thirdUpdateSql = "update orders set order_status = 'pending' where order_id = '949d5b44dbf5de918fe9c16f97b45f8a' ";
			third = remoteStmt.executeUpdate(thirdUpdateSql);
			//writeLock = false;
			System.out.println("SU3");
			}
		
			notifyAll();
			if(!orderLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(!productLock) {
			productLock = true;
			String fourthUpdateSql = "insert into productCategoryNameTranslation values ('maya' , 'magic')";
			fourth = remoteStmt.executeUpdate(fourthUpdateSql);
			System.out.println("SI4");
			
			String fifthUpdateSql = "insert into productCategoryNameTranslation values ('harsh' , 'happy')";
			fifth = remoteStmt.executeUpdate(fifthUpdateSql);
			System.out.println("SI5");
			
			}
			
			notifyAll();
			if(!productLock) {
				
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			if(first != 0 && second != 0 && third !=0 && fourth != 0 && fifth != 0)
			{
				try {
			localCon.commit();
			System.out.println("Local");
			secondLocalCommit = true;
			remoteCon.commit();
			secondRemoteCommit = true;
			System.out.println("Remote");
				}
				
				catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				finally {
					if(!secondLocalCommit || !secondRemoteCommit)
					{
							localCon.rollback();
							remoteCon.rollback();
					}
					
					geolocationLock = false;
					customerLock = false;
					orderLock = false;
					productLock = false;
				}
			}
			
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void closeConnection() {
			if(localCon != null) {
				try {
					localCon.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(remoteCon != null) {
				try {
					remoteCon.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(localStmt != null) {
				try {
					localStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(remoteStmt != null) {
				try {
					remoteStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}



	
	
	
}
