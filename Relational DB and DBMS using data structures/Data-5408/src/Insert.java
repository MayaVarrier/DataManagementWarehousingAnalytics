import org.json.simple.JSONArray;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Insert {
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public void insertQuery(String sql, String database) {
        boolean insert = false;
        sql = sql.trim();
        sql = sql.replaceAll("[^a-zA-Z0-9]"," ");
        String[] query = sql.split("\\s+");
        String table = query[2];

        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<String> columnValues = new ArrayList<>();
        int i=3;
        while (!query[i].equalsIgnoreCase("VALUES")){
            columnNames.add(query[i]);
            i++;
        }
        i++;
        while (i < query.length){
            columnValues.add(query[i]);
            i++;
        }
        JSONParser parser = new JSONParser();
        try{
            FileReader reader = new FileReader("Data-5408/files/" +database+ "/" +table+ ".json");
            Object obj = parser.parse(reader);
            JSONArray dataVal = (JSONArray) ((JSONObject) obj).get("dataVal");
            //JSONObject primaryKey = (JSONObject) ((JSONObject) obj).get("Primary Key");
            String primaryKey = (String) ((JSONObject) obj).get("Primary Key");
            ArrayList<String> columnValuespk = new ArrayList<>();

            if(dataVal.size()==0) {
                for (int j=0; j<columnValues.size();){
                    JSONObject rowVal = new JSONObject();
                    for(int k=0; k<columnNames.size();k++){
                        rowVal.put(columnNames.get(k), columnValues.get(j));
                        j++;
                    }
                    dataVal.add(rowVal);
                }

                ((JSONObject) obj).put("dataVal", dataVal);
                FileWriter writer = new FileWriter("Data-5408/files/" +database+ "/" +table+ ".json");
                writer.write(obj.toString());
                writer.flush();
            }
            else {
                for (int n = 0; n < dataVal.size(); n++) {
                    JSONObject data = (JSONObject) dataVal.get(n);
                    columnValuespk.add((String) data.get(primaryKey));
                }

                for (int j=0; j<columnValues.size();){
                    JSONObject rowVal = new JSONObject();
                    for(int k=0; k<columnNames.size();k++){
                        if(columnNames.get(k).equalsIgnoreCase(primaryKey)) {
                            if(!columnValuespk.contains(columnValues.get(j))) {
                                insert = true;
                            }
                        }
                        rowVal.put(columnNames.get(k), columnValues.get(j));
                        j++;
                    }
                    if(insert == true) {
                        dataVal.add(rowVal);
                    } else {
                        System.out.println("Primary key values should be unique.");
                    }

                }
                ((JSONObject) obj).put("dataVal", dataVal);
                FileWriter writer = new FileWriter("Data-5408/files/" +database+ "/" +table+ ".json");
                writer.write(obj.toString());
                writer.flush();
            }


            LOGGER.log(Level.INFO, "Values at for " +table+ " inserted at: " +database);
        } catch (IOException | ParseException e){
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "EXCEPTION", e);
        }
    }
}
