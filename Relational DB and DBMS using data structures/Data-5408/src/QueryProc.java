import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryProc{
    String databasename = null;

    boolean exit = true;
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public void QProcess(String sql, String database, ArrayList<String> createTableQueries)
    {
        if(sql.toUpperCase().contains("BEGIN") && sql.toUpperCase().contains("TRANSACTION")){
        Transactions t;
        try{
            t = new Transactions();
            t.transaction(sql, database);
        }catch(Exception e){
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
        }
    }else if(sql.toUpperCase().contains("CREATE") && sql.toUpperCase().contains("DATABASE"))
        {
            Database d;
            try{
                d = new Database();
                d.createDatabase(sql);
            }catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else if (sql.toUpperCase().contains("SHOW") && sql.toUpperCase().contains("TABLES")){
            Database d1;
            try{
                d1 = new Database();
                d1.showTables(database);
            }catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else if (sql.toUpperCase().contains("DROP") && sql.toUpperCase().contains("DATABASE")){
            Database d2;
            try{
                d2 = new Database();
                d2.dropSchema(database);
            }catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else if (sql.toUpperCase().contains("DROP") && sql.toUpperCase().contains("TABLE")){
            Database d3;
            try{
                d3 = new Database();
                d3.dropTables(database);
            }catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else if (sql.toUpperCase().contains("CREATE") && sql.toUpperCase().contains("TABLE") && exit){
            Create c;
            try{
                c = new Create();
                c.createTable(sql, database, createTableQueries);
            } catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        } else if (sql.toUpperCase().contains("INSERT")){
            Insert i;
            try{
                i = new Insert();
                i.insertQuery(sql, database);
            } catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        } else if(sql.toUpperCase().contains("SELECT") && sql.toUpperCase().contains("FROM")){
            Select s;
            try{
                s = new Select();
                s.selectCommand(sql, database);
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }else if(sql.toUpperCase().contains("DELETE") && sql.toUpperCase().contains("FROM")){
            Delete d;
            try{
                d = new Delete();
                d.deleteCommand(sql, database);
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        } else if(sql.toUpperCase().contains("UPDATE") && sql.toUpperCase().contains("SET") && sql.toUpperCase().contains("WHERE")){
            Update u;
            try{
                u = new Update();
                u.update(sql, database);
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        } else if(sql.toUpperCase().contains("UPDATE") && sql.toUpperCase().contains("SET")){
            Update u;
            try{
                u = new Update();
                u.update(sql, database);
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else if(sql.toUpperCase().contains("SHOWERD")){
            Erd erd;
            try{
                erd = new Erd();
                erd.showErd(sql, database);
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Warning logs at: " +QueryProc.class.getSimpleName());
            }
        }
        else {
            System.out.println("Please check your syntax");
            LOGGER.log(Level.WARNING, "Check your syntax for Query at : " +QueryProc.class.getSimpleName());
        }
    }
}

