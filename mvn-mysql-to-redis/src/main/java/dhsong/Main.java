package dhsong;

import dhsong.ClientConnection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import dhsong.SQL;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;



public class Main {

    public static void setup(ClientConnection client, String base, String database){
        client.sendCommand(Command.HSET, SafeEncoder.encode(base), SafeEncoder.encode(database), SafeEncoder.encode("0"));
        Long ros_long;
        ros_long = client.getIntegerReply();
        //System.out.println(ros_long);

        String ros_str;
        client.sendCommand(Command.SET, SafeEncoder.encode(database), SafeEncoder.encode("0"));
        ros_str = client.getBulkReply();
        //System.out.println(ros_str);
    } 

    public static void main( String[] args )
    {   
        Scanner sc = new Scanner(System.in);
        String BASE = "MySQL";
        String DATABASE = "base";
        ClientConnection client = new ClientConnection();

        Parser parse = new Parser();

        String ros_str = new String();
        List<String> ros_list = new ArrayList<>();
        List<String> ros_long;

        client.sendCommand(Command.KEYS, SafeEncoder.encode(BASE));
        ros_list = client.getMultiBulkReply();

        if(ros_list.isEmpty()){
            setup(client, BASE, DATABASE);
        }
    
        while(true){
            System.out.println("\n\nEnter the MySQL Command Below: ");
            System.out.println("[MySQL-to-Redis]  ");
            String command = sc.nextLine();
            String[] command_split = command.split(" ");
            command_split[0] = command_split[0].toLowerCase();
            String op = command_split[0];
            
            
            if(op.equals("show")){
                
                SQL.sqlShow(client, BASE, DATABASE);
            }
            else if(op.equals("create")){

                ArrayList<String> parsed = parse.parseCreate(command);

                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table Name", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Attribute", parsed.get(1));
                //System.out.println();
                //System.out.printf("%s >> %s", "Type", parsed.get(2));
                //System.out.println();

                SQL.sqlCreate(client, BASE, DATABASE, parsed.get(0), parsed.get(1), parsed.get(2));
            }
            else if(op.equals("insert")){

                ArrayList<String> parsed = parse.parseInsert(command);

                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table Name", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Value", parsed.get(1));
                //System.out.println();

                SQL.sqlInsert(client, BASE, DATABASE, parsed.get(0), parsed.get(1));
            }
            else if(op.equals("select")){
                String[] select_from_where = command.split(" ");
                boolean group = false;
                int idx = 0;
                while(idx <= select_from_where.length - 2){
                    if(select_from_where[idx].toLowerCase().equals("group") && select_from_where[idx + 1].toLowerCase().equals("by")){
                        group = true;
                        break;
                    }
                    idx += 1;
                }
        
                ArrayList<String> parsed = new ArrayList<>();
                if(group){
                    parsed = parse.parseGroup(command);
                    System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                    System.out.println();
                    System.out.printf("%s >> %s", "Table", parsed.get(0));
                    System.out.println();
                    System.out.printf("%s >> %s", "Attribute", parsed.get(1));
                    System.out.println();
                    System.out.printf("%s >> %s", "Condition", parsed.get(2));
                    System.out.println();
                    System.out.printf("%s >> %s", "Group", parsed.get(3));
                    System.out.println();
                }
                else{
                    parsed = parse.parseSelect(command);
                    System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                    System.out.println();
                    System.out.printf("%s >> %s", "Table", parsed.get(0));
                    System.out.println();
                    System.out.printf("%s >> %s", "Attribute", parsed.get(1));
                    System.out.println();
                    System.out.printf("%s >> %s", "Condition", parsed.get(2));
                    System.out.println();
                }

                //client.sendCommand(Command.SQLSELECT, database_name, tables, attributes, conditions);
                //result = client.getBulkReply();
            }
            else if(op.equals("update")){

                ArrayList<String> parsed = parse.parseUpdate(command);

                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Update", parsed.get(1));
                //System.out.println();
                //System.out.printf("%s >> %s", "Condition", parsed.get(2));
                //System.out.println();

                SQL.sqlUpdate(client, BASE, DATABASE, parsed.get(0), parsed.get(1), parsed.get(2));
            }
            else if(op.equals("delete")){

                ArrayList<String> parsed = parse.parseDelete(command);
 
                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Condition", parsed.get(1));
                //System.out.println();

                SQL.sqlDelete(client, BASE, DATABASE, parsed.get(0), parsed.get(1));
            }
            else if(op.equals("exit")){
                break;
            }
        }

        client.close();

    }
}
