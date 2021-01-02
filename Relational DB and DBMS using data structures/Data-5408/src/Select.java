import org.json.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Select {
    public void selectCommand(String sql, String database) throws IOException {
		 try{
		        String[] splited = sql.split(" ");
		        int i=0;
		        String[] filter=splited[1].split(",");
		        String count="";
		        if(splited[1].equalsIgnoreCase("count(*)")) {
		        	count="*";
		        }
		        while (!splited[i].equalsIgnoreCase("from")){
		            i++;
		        }
		        String table=splited[i+1];
		        String path="Data-5408/src/files/" +database+ "/" +table+ ".json";
		        File tableFile = new File(path);
		        
		        if(tableFile.exists()){
			        InputStream is = new FileInputStream(path);  
	
	                JSONTokener tokener = new JSONTokener(is);
	                JSONObject obj = new JSONObject(tokener);
					JSONArray datalist =  (JSONArray) obj.get("dataVal"); 
					
					ArrayList<String> columnNames = new ArrayList<>();
					ArrayList<String> operatorValues = new ArrayList<>();
		            ArrayList<String> columnValues = new ArrayList<>();
		            ArrayList<String> bigOperatorValues = new ArrayList<>();
		           
			        if(sql.contains("WHERE")){
			            int i3=0;
			            while (!splited[i3].equalsIgnoreCase("where")){
			                i3++;
			            }
			            i3++;
			            while (splited.length>i3){
			            String colCondition = splited[i3];
			            String operator = null;
			            if(colCondition.contains("<=")) {		            	
			            	operator="<=";		            	
			            }
			            else if(colCondition.contains(">=")) {
			            	operator=">=";
			            }
			            else if(colCondition.contains("=")) {
			            	operator="=";
			            }
			            else if(colCondition.contains("!=")) {
			            	operator="!=";
			            }
			            if((splited[i3].equalsIgnoreCase("or"))||(splited[i3].equalsIgnoreCase("and"))) {
			            	bigOperatorValues.add(splited[i3]);
			            }
			            else {
				            if(operator==null) {
				            	columnNames.add(splited[i3+1]);
				            	operatorValues.add(operator);	
			                    columnValues.add(splited[i3+3]);		
				            }
				            else {
				            String[] cols = colCondition.split(operator);
			           		 if(cols.length==2) {
				            		 try {		            		 
					                     columnNames.add(cols[0]);
					                     columnValues.add(cols[1]);	
					                     operatorValues.add(operator);	
				            		 }
				            		 catch (Exception e){
				            	         e.printStackTrace();
				            	         System.out.println();
			           	         } 
			           		 }		           		
			            }
				            if(splited.length<i3+3)
				           		 break;
					            }
			            	i3++;
			           } 
			            
			            JSONObject objectInArrayB = (JSONObject) datalist.get(0);
			            String[] elementNamesB = JSONObject.getNames(objectInArrayB);
			            if(elementNamesB.length<columnNames.size()) {
			            	System.out.println("Invalid no.of parameters passed, Please check and retry!");
			            	
			            }
			            else if(elementNamesB.length>=columnNames.size()) {
				            int valid=0;
				            for (int j=0; j<columnNames.size();j++){
				            	if(!datalist.isNull(0)) {
					              JSONObject objectInArray = (JSONObject) datalist.get(0);
					              String[] elementNames = JSONObject.getNames(objectInArray);		              
					              for (String elementName : elementNames)
					              {
					            	  if(elementName.equals(columnNames.get(j))) {
					            		  valid++;
					            	  }
					              }
				            	}  
				            }
				            if(valid!=columnNames.size()) {
				            	System.out.println("Invalid query");
				            }
				            else if(valid==columnNames.size()) {
				            	int success=0,failed=0;
				            	ArrayList<Integer> results = new ArrayList<>();
				            	int m=0;
				            	for (int j=0; j<columnNames.size();j++){	
				            		for (int ii = 0, size =  datalist.length(); ii < size; ii++)
						            {
				            			if(!datalist.isNull(ii)) {	
						              JSONObject objectInArray = (JSONObject) datalist.get(ii);
						              String[] elementNames = JSONObject.getNames(objectInArray);		              
						              for (String elementName : elementNames)
						              {
						            	  String value = objectInArray.getString(elementName);
						            	  if((elementName.equalsIgnoreCase(columnNames.get(j)))&&(value.equalsIgnoreCase(columnValues.get(j)))) {
						            		  if(bigOperatorValues.size()!=0){
							            		  if((bigOperatorValues.get(m).equalsIgnoreCase("or"))&&(j>0)&&(m<bigOperatorValues.size())) {
								            		  results.add(ii);
								            		  success++;
								            		  m++;
							            		  }
							            		  else if((bigOperatorValues.get(m).equalsIgnoreCase("and"))&&(j>0)&&(m<bigOperatorValues.size())) {
								            		  if(results.contains(ii)){
								            			  for(int ij=0;ij<results.size();ij++) {
								            				  if(results.get(ij)!=ii) {
								            					  results.remove(ij);
								            				  }
								            			  }
								            			  success++;
								            		  }else {
								            			  failed++;
								            		  }
								            		  m++;
							            		  }
							            		  else {
							            			  results.add(ii);
								            		  success++;
							            		  }
						            		  }
						            		  else {
						            			  results.add(ii);
							            		  success++;
						            		  }
						            	  }
						              }
				            			}
						            }
					            }
				            	if((success>=columnValues.size())&&(success>=1)&&(failed==0)) {
				            		if(!datalist.isNull(0)) {
				            		JSONObject objectInArrayA = (JSONObject) datalist.get(0);
			      		              String[] elementNamesA = JSONObject.getNames(objectInArrayA);
				      		          List<String> finalResultNames = new ArrayList<>();
			      		              int ik=0,print=0;
			      		              for(ik=0;ik<filter.length;ik++) {
				      		              for (String elementName : elementNamesA)
				      		              {
				      		            	  if(filter[ik].equalsIgnoreCase(elementName)||(filter[ik].equals("*")||(count.equals("*")))) {
				      		            		if(count.equals("*")){
					      		            		 if(print==0) {
					      		            			System.out.println("count");
					      		            			print=1;
					      		            		  }
					      		            	 } else {
					      		            			System.out.print(elementName+"\t\t");				      		            			  
					      		            		  }
				      		            		  finalResultNames.add(elementName);
				      		            	  }
				      		              }
			      		              } 
			      		            if((finalResultNames.size()>0)||(count=="*")) { 
			      		            	if(count!="*") {
				      		              System.out.println(); 
			      		            	}
			      		            	int print1=0;
					            		for(int j=0;j<results.size();j++) {
					            			JSONObject objectInArrayC = (JSONObject) datalist.get(results.get(j));
					      		              List<String> elementNamesC = finalResultNames;
					      		              for (String elementName : elementNamesC)
					      		              {
						      		            	String value = objectInArrayC.getString(elementName);
						      		            	 if(count.equals("*")){
						      		            		 if(print1==0) {
						      		            			System.out.println(results.size());
						      		            			print1=1;
						      		            		  }
						      		            	 } else {
						      		            			System.out.print(value+"\t\t");				      		            			  
						      		            		  }
					      		              }
					      		             if(count!="*") { 
					      		            	 System.out.println();
					      		             }
					            		}
					            		System.out.println(results.size()+" Records found, Query executed successfully");
			      		            }else {
			      		            	System.out.println("Syntax error please check!");
			      		            }	
				            	}else{
		            				System.out.println("No records found!");
		            			}
				            }
				        }    	
	                }
			            
			        } else {		 
			        	if(!datalist.isNull(0)) {
			        	JSONObject objectInArrayA = (JSONObject) datalist.get(0);
			              String[] elementNamesA = JSONObject.getNames(objectInArrayA);			              
			              List<String> finalResultNames = new ArrayList<>();
			              int ik=0,print=0,print1=0;
      		              for(ik=0;ik<filter.length;ik++) {
	      		              for (String elementName : elementNamesA)
	      		              {
	      		            	  if(filter[ik].equalsIgnoreCase(elementName)||(filter[ik].equals("*"))||(count.equals("*"))) {
	      		            		if(count.equals("*")){
		      		            		 if(print==0) {
		      		            			System.out.println("count");
		      		            			print=1;
		      		            		  }
		      		            	 } else {
		      		            			System.out.print(elementName+"\t\t");				      		            			  
		      		            		  }
	      		            		  finalResultNames.add(elementName);
	      		            	  }
	      		              }
      		              }
      		            
      		             if(finalResultNames.size()>0) { 
      		            	if(count!="*") { 
	      		            	 System.out.println();
	      		             }
				            for (int ii = 0, size =  datalist.length(); ii < size; ii++)
				            {
				              JSONObject objectInArray = (JSONObject) datalist.get(ii);
				              List<String> elementNames = finalResultNames;		              
				              for (String elementName : elementNames)
				              {
				                String value = objectInArray.getString(elementName);
				                if(count.equals("*")){
	      		            		 if(print1==0) {
	      		            			System.out.println(datalist.length());
	      		            			print1=1;
	      		            		  }
	      		            	 } else {
	      		            			System.out.print(value+"\t\t");				      		            			  
	      		            		  }
				              }
				              if(count!="*") { 
		      		            	 System.out.println();
		      		             }
				            }
				              System.out.println("\n"+datalist.length()+" Records found in table : "+table);
      		             }else {
      		            	 System.out.println("Syntax error, Please check!");
      		             }
			        } 
			    }   	
		 	}else {
		 		System.out.println(table+" table not found, please check and retry!");
		 	}	
		 } catch (Exception e){
				e.printStackTrace();
			} 
    }
}
