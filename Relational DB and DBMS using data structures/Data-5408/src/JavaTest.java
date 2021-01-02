//import at.favre.lib.crypto.bcrypt.BCrypt;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaTest {
    private final static java.util.logging.Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static String hashPassword(String plainTextPassword){
    try{
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(plainTextPassword.getBytes());
        byte[] resultbyArray = digest.digest();
        StringBuilder sb = new StringBuilder();
        for(byte b : resultbyArray){
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }catch (NoSuchAlgorithmException e){
        e.printStackTrace();
    }
    return "";
    }
    public void authenticate() throws IOException{
        LOGGER.log(Level.INFO, "Information logs at class: " + JavaTest.class.getSimpleName());
        String greeting = "Hello";
        String username;
        String password = null;
        int choice = 0;
        int exitOption = 0;

        // Used to hold the instance of a user who successfully logged in
        User loggedInUser = null;

        // Get user input
        BufferedReader br = new BufferedReader(
                new InputStreamReader(System.in));
        Scanner sc = new Scanner(System.in);

        System.out.println("*** Welcome to the Database  ***\n");
        System.out.println(greeting);

        while (exitOption == 0) {
            System.out.println("Please choose from the below options : \n 1. Create an account \n 2. Login");
            choice = sc.nextInt();
            if (choice == 1) {
                JSONObject obj = new JSONObject();
                JSONArray array = new JSONArray();
                JSONObject userinfo = new JSONObject();
                System.out.println("Please type your username :");
                username = br.readLine();
                System.out.println("This will be the username you will use to log in.");
                System.out.println("Please type your password :");
                password = br.readLine();
                obj.put("username", username);
                obj.put("password", hashPassword(password));
                array.put(obj);

                try {
                    JSONParser jsonParser = new JSONParser();
                    File file = new File("Data-5408/files/userInfo.json");

                    if(file.length() == 0) {
                        FileWriter writer = new FileWriter("Data-5408/files/userInfo.json");
                        writer.write(array.toString());
                        writer.close();
                    }
                    else {
                        FileReader reader = new FileReader("Data-5408/files/userInfo.json");
                        Object object = jsonParser.parse(reader);
                        org.json.simple.JSONArray userList = (org.json.simple.JSONArray) object;
                        userList.add(obj);
                        FileWriter writer = new FileWriter("Data-5408/files/userInfo.json");
                        writer.write(userList.toString());
                        writer.flush();
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                LOGGER.log(Level.INFO, "New user created.");
            } else if (choice == 2) {
                System.out.println("Please type your username :");
                username = br.readLine();
                System.out.println("Please type your password :");
                password = br.readLine();

                try {
                    JSONParser jsonParser = new JSONParser();
                    FileReader reader = new FileReader("Data-5408/files/userInfo.json");
                    Object obj = jsonParser.parse(reader);
                    org.json.simple.JSONArray userList = (org.json.simple.JSONArray) obj;

                    for (int i = 0; i < userList.size(); i++) {
                        org.json.simple.JSONObject object = (org.json.simple.JSONObject) userList.get(i);
                        if (username.equals(object.get("username"))) {
                            if (hashPassword(password).equals(object.get("password"))) {
                                loggedInUser = new User(username, password);
                                exitOption = 1;
                                // when a user is found, "break" stops iterating through the list
                                break;
                            }
                        }
                        exitOption = 1;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else {
                exitOption = 1;
                System.out.println("Wrong option.");
            }
        }

        try{
            // if loggedInUser was changed from null, it was successful
            if (loggedInUser != null)
            {
                System.out.println("User successfully logged in: "+loggedInUser.getUsername());
                LOGGER.log(Level.INFO, "User Logged in successfully!!");
                QueryExec exec = new QueryExec();
                exec.QueryExecution();
            }
            else
            {
                System.out.println("Invalid username/password combination");
                LOGGER.log(Level.SEVERE, "Wrong Username-Password for class: " +JavaTest.class.getSimpleName());
            }
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Catching Exception" +JavaTest.class.getSimpleName());
        }

    }
}
