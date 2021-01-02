import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Create {
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public void createTable(String sql, String database, ArrayList<String> createTableQueries) throws JSONException {

        sql = sql.trim();
        String sqlpk = sql;
        String sqlfk = sql;
        sql = sql.replaceAll("[^a-zA-Z0-9]"," ");
        String[] query = sql.split("\\s+");
        String table = query[2];

        LinkedList<String> columns = new LinkedList<>();
        for(int i=3; i< query.length-1; i=i+2){
            if(query[i].equalsIgnoreCase("PRIMARY"))
            {
                break;
            }
            columns.add(query[i]);
        }

        JSONObject obj = new JSONObject();
        JSONArray datalistA = new JSONArray();
        obj.put("tablename", table);
        obj.put("dataVal", datalistA);
        obj.put("createQuery", sql);
        JSONArray arrayElementOneArray = new JSONArray();
        for(int i=0; i<columns.size();i++) {
            JSONObject objectElement = new JSONObject();
            objectElement.put("Column"+i,columns.get(i));
            arrayElementOneArray.put(objectElement);
        }
        obj.put("columnlist", arrayElementOneArray);

        if(sqlpk.toUpperCase().contains("PRIMARY KEY")){
            String[] querypk = sql.split("\\s+");
            int i = 0;
            while (!querypk[i].equalsIgnoreCase("PRIMARY")) {
                i++;
            }
            ArrayList<String> primaryKeys = new ArrayList<>();
            primaryKeys.add(querypk[i+2]);

            for(int j=0;j<primaryKeys.size();j++) {
                obj.put("Primary Key",primaryKeys.get(j));
            }
        }

        if(sqlfk.toUpperCase().contains("FOREIGN KEY")){
            String[] queryfk = sql.split("\\s+");
            int i = 0;
            while (!queryfk[i].equalsIgnoreCase("FOREIGN")) {
                i++;
            }

            JSONArray foreignKeyArray = new JSONArray();
            JSONObject foreignKey = new JSONObject();
            foreignKey.put("Foreign Key",queryfk[i+2]);
            foreignKeyArray.put(foreignKey);
            JSONObject refTable = new JSONObject();
            refTable.put("Referencing Table",queryfk[i+4]);
            foreignKeyArray.put(refTable);
            JSONObject refColumn = new JSONObject();
            refColumn.put("Referencing Column",queryfk[i+5]);
            foreignKeyArray.put(refColumn);

            obj.put("Foreign Key Details", foreignKeyArray);

        }

        try{
            File file = new File("Data-5408/files/" +database+ "/" +table+ ".json");
            file.createNewFile();
            FileWriter writer = new FileWriter(file);
            writer.write(obj.toString());
            writer.close();
            createTableQueries.add(sql);
            System.out.println(createTableQueries.toString());
            LOGGER.log(Level.INFO, "Table " +table+ " created at: " +database);
        } catch (IOException e){
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Warning logs at: " +Create.class.getSimpleName());
        }
    }
}
