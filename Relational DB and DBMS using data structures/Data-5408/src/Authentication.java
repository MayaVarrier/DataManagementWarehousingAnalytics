import java.util.Scanner;

public class Authentication {
    
    public static void main(String[] args) throws Exception{
        String database = null;
        String sql=null;
        JavaTest test = new JavaTest();
        DataDump dump = new DataDump();
        Scanner scan = new Scanner(System.in);
        System.out.println("Available: 1.Authentication 2. Dump Creation");
        System.out.println("Enter your choice");
        int choice = scan.nextInt();
        switch (choice){
            case 1:
                test.authenticate();
                break;
            case 2:
                dump.dumpCreation("Data-5408/file/"+database);
                break;
            default:
                System.out.println("invalid choice");
        }
    }
}


