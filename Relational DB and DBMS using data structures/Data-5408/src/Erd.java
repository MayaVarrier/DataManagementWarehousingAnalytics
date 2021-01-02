import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;

import org.json.*;
import org.json.simple.parser.JSONParser;

public class Erd {
	public void showErd(String sql, String database) {
		try{
	        String[] splited = sql.split(" ");
	        if(splited[0].equalsIgnoreCase("showerd"))
	        {
	        	String dbpath="Data-5408/files/" +database;
		        File databaseFile = new File(dbpath);
		        
		        if(databaseFile.exists()){
		        	File[] tables = databaseFile.listFiles();
		        	String tablenames[] = databaseFile.list();
		        	if(tables.length!=0) {
		                JSONParser parser = new JSONParser();
		                for (int i = 0; i < tables.length; i++) {
		                    if (tables[i].isFile() && tables[i].getName().contains(".json")) {
		                        InputStream is = new FileInputStream("Data-5408/files/" +database+"/"+tables[i].getName());  
		                    	
		    	                JSONTokener tokener = new JSONTokener(is);
		    	                JSONObject obj = new JSONObject(tokener);
		    	                System.out.println("Table : "+tablenames[i].replaceFirst("[.][^.]+$", ""));
		    	                System.out.println(obj.get("columnlist"));
		    	                if(obj.has("Primary Key")) {
		    	                System.out.println("Primary key : "+obj.get("Primary Key"));
		    	                }
		    	                else {
		    	                	System.out.println("No relations found with this table");
		    	                }
		    	                if(obj.has("Foreign Key Details")) {
		    	                	JSONArray fk=obj.getJSONArray("Foreign Key Details");
		    	                	
		    	                System.out.println("Foreign key Details:");
			    	                for(int k=0;k<fk.length();k++) {
			    	                	System.out.println(fk.get(k));
			    	                }
		    	                }
			                    System.out.println("");
		                    }
		                }
		        	}else {
		        		System.out.println("No tables found in "+database);
		        	}
		        }else {
		        	System.out.println("Database not found!");
		        }	        
	        }
		}
		catch (Exception e){
			e.printStackTrace();
		} 
	}
}
