import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Transactions {

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public void transaction(String sql, String database) throws IOException {

        sql = sql.trim();
        sql = sql.replaceAll("[;,),(]", " ");
        String[] query = sql.split("\\s+");
        String table = "";
        Boolean isModified = false;

        if(sql.toUpperCase().contains("INSERT")) {
            table = query[5];
        } else if (sql.toUpperCase().contains("UPDATE")) {
            table = query[4];
        } else if (sql.toUpperCase().contains("SELECT")) {
            int i = 3;
            while (!query[i].equalsIgnoreCase("FROM")){
                i++;
            }
            table = query[i+1];
        } else if (sql.toUpperCase().contains("DELETE")) {
            int i = 3;
            while (!query[i].equalsIgnoreCase("FROM")){
                i++;
            }
            table = query[i+1];
        }

        try{
            JSONParser jsonParser = new JSONParser();
            File file = new File("Data-5408/files/transactions.json");

            if(!(file.length() == 0)) {
                FileReader reader = new FileReader("Data-5408/files/transactions.json");
                Object object = jsonParser.parse(reader);
                org.json.simple.JSONArray transactionList = (org.json.simple.JSONArray) object;

                for(int i=0;i<transactionList.size();i++) {
                    org.json.simple.JSONObject obj = (org.json.simple.JSONObject) transactionList.get(i);
                    if(table.equalsIgnoreCase((String) obj.get("Table Name"))) {
                        isModified = true;
                    }
                }
            }

            if(isModified == true) {
                System.out.println("The transaction cannot be executed because the table is being modified by another user.");
            } else {

                File transactionFile = new File("Data-5408/files/transactions.json");
                if(transactionFile.length() == 0) {
                    JSONArray transactionArray = new JSONArray();
                    JSONObject tableName = new JSONObject();
                    tableName.put("Table Name",table);
                    transactionArray.add(tableName);
                    FileWriter writer = new FileWriter("Data-5408/files/transactions.json");
                    writer.write(transactionArray.toString());
                    writer.close();
                } else {
                    JSONObject tableName = new JSONObject();
                    tableName.put("Table Name",table);
                    FileReader reader = new FileReader("Data-5408/files/transactions.json");
                    Object object = jsonParser.parse(reader);
                    org.json.simple.JSONArray transactionList = (org.json.simple.JSONArray) object;
                    transactionList.add(tableName);
                    FileWriter writer = new FileWriter("Data-5408/files/transactions.json");
                    writer.write(transactionList.toString());
                    writer.flush();
                }

                int i = 0;
                    while(!query[i].equalsIgnoreCase("COMMIT")) {
                        i++;
                        while (query[i].equalsIgnoreCase("UPDATE") || query[i].equalsIgnoreCase("SELECT") ||
                                query[i].equalsIgnoreCase("INSERT") || query[i].equalsIgnoreCase("DELETE")) {

                            if (query[i].equalsIgnoreCase("INSERT")){
                                Insert insert;
                                try{
                                    insert = new Insert();
                                    int j = i;
                                    String insertQuery = "";
                                    while(!query[j].equalsIgnoreCase("UPDATE") && !query[j].equalsIgnoreCase("SELECT")&&
                                            !query[j].equalsIgnoreCase("DELETE") && !query[j].equalsIgnoreCase("COMMIT")) {
                                        insertQuery = insertQuery+" "+query[j];
                                        j++;
                                    }

                                    insert.insertQuery(insertQuery, database);
                                } catch (Exception e){
                                    e.printStackTrace();
                                    LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
                                }finally {
                                    i++;
                                }
                            }else if(query[i].equalsIgnoreCase("UPDATE") && sql.toUpperCase().contains("SET")){
                                Update u;
                                try{
                                    u = new Update();
                                    int j = i;
                                    String updateQuery = "";
                                    while(!query[j].equalsIgnoreCase("INSERT")&& !query[j].equalsIgnoreCase("SELECT")&&
                                            !query[j].equalsIgnoreCase("DELETE") && !query[j].equalsIgnoreCase("COMMIT")) {
                                        updateQuery = updateQuery+" "+query[j];
                                        j++;
                                    }
                                    u.update(updateQuery, database);
                                }catch(Exception e){
                                    e.printStackTrace();
                                    LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
                                }finally {
                                    i++;
                                }
                            }else if(query[i].equalsIgnoreCase("SELECT") && sql.toUpperCase().contains("FROM")){
                                Select s;
                                try{
                                    s = new Select();
                                    int j = i;
                                    String selectQuery = "";
                                    while(!query[j].equalsIgnoreCase("INSERT")&& !query[j].equalsIgnoreCase("UPDATE")&&
                                            !query[j].equalsIgnoreCase("DELETE") && !query[j].equalsIgnoreCase("COMMIT")) {
                                        selectQuery = selectQuery+" "+query[j];
                                        j++;
                                    }
                                    s.selectCommand(selectQuery, database);
                                }catch(Exception e){
                                    e.printStackTrace();
                                    LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
                                }finally {
                                    i++;
                                }
                            }else if(query[i].equalsIgnoreCase("DELETE") && sql.toUpperCase().contains("FROM")){
                                Delete d;
                                try{
                                    d = new Delete();
                                    int j = i;
                                    String deleteQuery = "";
                                    while(!query[j].equalsIgnoreCase("INSERT")&& !query[j].equalsIgnoreCase("UPDATE")&&
                                            !query[j].equalsIgnoreCase("SELECT") && !query[j].equalsIgnoreCase("COMMIT")) {
                                        deleteQuery = deleteQuery+" "+query[j];
                                        j++;
                                    }
                                    d.deleteCommand(deleteQuery, database);
                                }catch(Exception e){
                                    e.printStackTrace();
                                    LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
                                }finally {
                                    i++;
                                }
                            }
                        }
                    }
                JSONObject tableName = new JSONObject();
                tableName.put("Table Name",table);
                FileReader reader = new FileReader("Data-5408/files/transactions.json");
                Object object = jsonParser.parse(reader);
                org.json.simple.JSONArray transactionList = (org.json.simple.JSONArray) object;
                transactionList.remove(tableName);
                FileWriter writer = new FileWriter("Data-5408/files/transactions.json");
                writer.write(transactionList.toString());
                writer.flush();
                System.out.println("Query ended");
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
