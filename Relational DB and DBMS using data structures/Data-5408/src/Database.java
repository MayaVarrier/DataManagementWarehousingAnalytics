import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database{
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    boolean exit  = true;
    public void createDatabase(String sql){
        sql = sql.trim();
        sql = sql.replaceAll("[^a-zA-Z0-9]", " ");
        String[] query = sql.split("\\s+");
        String databases = query[2];
        JSONParser jsonParser = new JSONParser();
        try{
            FileReader reader = new FileReader("Data-5408/files/databases.json");
            Object obj = jsonParser.parse(reader);
            JSONArray databaselist = (JSONArray) obj;

            databaselist.forEach(db -> {
                if (((JSONObject) db).get("dbname").equals(databases)){
                    System.out.println("Database already exists! Create new one");
                    LOGGER.log(Level.WARNING, "DATABASE " +databases+ " ALREADY EXISTS FOR " +Database.class.getSimpleName());
                    exit = false;
                }
            });
            if(exit) {
                JSONArray databaselist1;
                JSONObject json1 = new JSONObject();
                FileReader reader1 = new FileReader("Data-5408/files/databases.json");
                json1.put("dbname", databases);
                if(reader1==null){
                    databaselist1 = new JSONArray();
                    databaselist1.add(json1);
                    FileWriter writer = new FileWriter("Data-5408/files/databases.json");
                    writer.write(databaselist1.toString());
                    writer.flush();
                    LOGGER.log(Level.INFO, "DATABASE " +databases+ " CREATED FOR " +Database.class.getSimpleName());
                }
                else {
                    Object obj1 = jsonParser.parse(reader1);
                    JSONArray databaseListFinal = (JSONArray) obj1;
                    databaseListFinal.add(json1);
                    FileWriter file = new FileWriter("Data-5408/files/databases.json");
                    file.write(databaseListFinal.toString());
                    file.flush();
                    LOGGER.log(Level.INFO, "DATABASE " +databases+ " CREATED FOR" +Database.class.getSimpleName());
                }

                String path = "Data-5408/files/" +databases;
                File file1 = new File(path);
                System.out.println(path);
                boolean bool = file1.mkdir();
            }
        } catch (ParseException | IOException fe){
            fe.printStackTrace();
            LOGGER.log(Level.WARNING, "These are logs at warning for class" +Database.class.getSimpleName());
        }
    }

    public void showTables(String datbase){
        File file = new File("Data-5408/files/" +datbase);
        File[] Files = file.listFiles();
        for(int i=0; i<Files.length;i++){
            if(Files[i].isFile() && Files[i].getName().contains(".json")){
                System.out.println(Files[i].getName().replace(".json",""));
                LOGGER.log(Level.INFO, "TABLES FOR " +datbase);
            }
        }
    }

    public void dropTables(String database){
        File file = new File("Data-5408/files/" +database);
        File[] Files = file.listFiles();
        for(int i=0; i<Files.length;i++){
            if(Files[i].isFile() && Files[i].getName().contains(".json")){
                Files[i].delete();
                LOGGER.log(Level.WARNING, "TABLE FOR " +database+ " DROPPED ");
            }
        }
    }
     public String dropSchema(String database){
        File file = new File("Data-5408/files/" +database);
        File[] Files = file.listFiles();
        for(int i=0; i<Files.length; i++){
            Files[i].delete();
            LOGGER.log(Level.WARNING,  "DATABASE " +database+" DROPPED ");
        }
        file.delete();
        LOGGER.log(Level.WARNING,  database +" DROPPED ");
        return database +"deleted";
     }
}

