import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataDump {
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public void dumpCreation(String database) throws IOException {
        ArrayList<String> createTableQueries = new ArrayList<String>();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Select Database!");

        String sql = scanner.nextLine();
        sql = sql.toUpperCase();
        if(sql.contains("USE")) {
            sql = sql.trim();
            sql = sql.replaceAll("[^a-zA-Z0-9]", " ");
            String[] splited1 = sql.split("\\s+");
            String data = splited1[1];
            int i =0;
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
                File folder = new File("Data-5408/files/" +data);
                File[] listOfFiles = folder.listFiles();
                JSONParser parser = new JSONParser();
                for (int k = 0; k < listOfFiles.length; k++) {
                    if (listOfFiles[k].isFile() && listOfFiles[k].getName().contains(".json")) {
                        FileReader readernew = new FileReader(listOfFiles[k]);
                        Object objN = parser.parse(readernew);
                        createTableQueries.add((String) ((JSONObject) objN).get("createQuery"));
                    }
                }
                for(int k=0; k<createTableQueries.size(); k++){
                    System.out.println(createTableQueries.get(k)+"");
                    FileWriter writer = new FileWriter("Data-5408/files/"+data+"/dump.txt");
                    for(String str : createTableQueries){
                        writer.write(str + System.lineSeparator());
                    }
                    writer.close();
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
