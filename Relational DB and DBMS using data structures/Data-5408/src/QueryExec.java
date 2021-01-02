import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryExec {
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public void QueryExecution(){
        ArrayList<String> createTableQueries = new ArrayList<String>();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Select Database or Create a new one!");

        String sql = scanner.nextLine();
        sql = sql.toUpperCase();
        if(sql.contains("USE")) {
            sql = sql.trim();
            sql = sql.replaceAll("[^a-zA-Z0-9]", " ");
            String[] splited1 = sql.split("\\s+");
            String database = splited1[1];
            try{
                JSONParser jsonParser = new JSONParser();
                FileReader reader;
                reader = new FileReader("Data-5408/files/databases.json");
                Object obj;
                obj = jsonParser.parse(reader);

                JSONArray databaselist = (JSONArray) obj;
                databaselist.forEach(db -> {
                    if(((JSONObject) db).get("dbname").equals(database)){
                        System.out.println("Database exists");
                        LOGGER.log(Level.INFO, "This is my log at class: " +QueryExec.class.getSimpleName());
                    }
                });
                while (true){
                    System.out.println("Enter query or enter 0 to exit:");
                    String sqlnew = scanner.nextLine();
                    if(sqlnew.equals("0")){
                        System.out.println("Process Ended.");
                        LOGGER.log(Level.INFO, "Process ended for: " +QueryExec.class.getSimpleName());
                        break;
                    } else {
                        QueryProc q1 = new QueryProc();
                        q1.QProcess(sqlnew, database, createTableQueries);
                    }
                }
            } catch (IOException | ParseException e){
                LOGGER.log(Level.WARNING, "DATABASE FACED SOME ISSUES AT:" +QueryExec.class.getSimpleName());
                e.printStackTrace();
            }
        } else {
            QueryProc q = new QueryProc();
            q.QProcess(sql, null, createTableQueries);
            LOGGER.log(Level.INFO, "This is my log at warning at class" +QueryExec.class.getSimpleName());
        }
    }
}
