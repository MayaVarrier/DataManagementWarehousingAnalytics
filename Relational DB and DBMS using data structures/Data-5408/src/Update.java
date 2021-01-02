import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Update {
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public void update(String sql, String database) throws IOException {
        sql = sql.trim();
        sql = sql.replaceAll("[;]", " ");
        String[] splited = sql.split("\\s+");
        int i = 0;
        while (!splited[i].equalsIgnoreCase("UPDATE")) {
            i++;
        }
        String table = splited[i + 1];

        if (sql.contains("WHERE") || sql.contains("where")) {
            int j = 0;
            int k = 0;
            String whereOperator;
            String whereColCondition;
            String whereValCondition;
            String colCondition;
            String operator;
            String valCondition;

            while (!splited[k].equalsIgnoreCase("WHERE")) {
                k++;
            }
            if (k <= splited.length) {
                whereColCondition = splited[k + 1];
                whereOperator = splited[k + 2];
                whereValCondition = splited[k + 3];

                while (!splited[j].equalsIgnoreCase("SET")) {
                    j++;
                }
                if (j <= splited.length) {
                    colCondition = splited[j + 1];
                    operator = splited[j + 2];
                    valCondition = splited[j + 3];

                    JSONParser parser = new JSONParser();
                    try {
                        FileReader reader = new FileReader("Data-5408/files/" + database + "/" + table + ".json");
                        Object obj = parser.parse(reader);
                        JSONArray datalist = new JSONArray();
                        datalist = (JSONArray) ((JSONObject) obj).get("dataVal");

                        for (int n = 0; n < datalist.size(); n++) {
                            if (whereOperator.equals("=")) {
                                JSONObject data = (JSONObject) datalist.get(n);
                                if (data.get(whereColCondition).equals(whereValCondition)) {
                                    data.put(colCondition, valCondition);
                                }
                            }
                        }
                        ((JSONObject) obj).put("dataVal", datalist);
                        FileWriter writer = new FileWriter("Data-5408/files/" + database + "/" + table + ".json");
                        writer.write(obj.toString());
                        writer.flush();
                        LOGGER.log(Level.INFO, "Table " +table+ " Updated at: " +database);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        LOGGER.log(Level.WARNING, "EXCEPTION", e);
                    }
                }
            }

        } else {

            int j = 0;
            while (!splited[j].equalsIgnoreCase("SET")) {
                j++;
            }

            if (j <= splited.length) {
                String colCondition = splited[j + 1];
                String operator = splited[j + 2];
                String valCondition = splited[j + 3];

                JSONParser parser = new JSONParser();
                try {
                    FileReader reader = new FileReader("Data-5408/files/" + database + "/" + table + ".json");
                    Object obj = parser.parse(reader);
                    JSONArray columnlist = new JSONArray();
                    columnlist.add(((JSONObject) obj).get("columnlist"));
                    JSONObject columns = (JSONObject) columnlist.get(0);
                    Set<String> keys = columns.keySet();
                    JSONArray datalist = new JSONArray();
                    datalist = (JSONArray) ((JSONObject) obj).get("dataVal");

                    for (int n = 0; n < datalist.size(); n++) {
                        JSONObject data = (JSONObject) datalist.get(n);
                        if (operator.equals("=")) {
                            if (data.keySet().contains(colCondition)) {
                                data.put(colCondition, valCondition);
                            }
                        }
                    }
                    ((JSONObject) obj).put("dataVal", datalist);
                    FileWriter writer = new FileWriter("Data-5408/files/" + database + "/" + table + ".json");
                    writer.write(obj.toString());
                    writer.flush();
                    LOGGER.log(Level.INFO, "Table " +table+ " Updated at: " +database);
                } catch (ParseException e) {
                    e.printStackTrace();
                    LOGGER.log(Level.WARNING, "EXCEPTION", e);
                }
            } else {
                System.out.println("Query ended.");
            }
        }
    }
}
