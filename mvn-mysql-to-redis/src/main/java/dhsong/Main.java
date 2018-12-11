package dhsong;

import dhsong.Connection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


class ClientConnection extends Connection{
    public ClientConnection(){
        this("localhost", 6379);
    }
    public ClientConnection(String ip){
        this(ip, 6379);
    }
    public ClientConnection(String ip, int port){
        super(ip, port);
    }

    public void sendCommand(final ProtocolCommand cmd, final byte[]... args){
        super.sendCommand(cmd, args);
    }


}

public class Main 
{ 
    public static void main( String[] args )
    {   
        Scanner sc = new Scanner(System.in);
        String BASE = "MySQL";
        String DATABASE = "database";
        ClientConnection client = new ClientConnection();

        Parser parse = new Parser();

        String result;
        List<String> result_list = new ArrayList<>();
    
        while(true){
            System.out.println("Enter the MySQL Command Below: ");
            System.out.print("[MySQL-to-Redis]");
            String command = sc.nextLine();
            String[] command_split = command.split(" ");
            command_split[0] = command_split[0].toLowerCase();
            String op = command_split[0];
            
            
            if(op.equals("show")){
                
                System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                System.out.println();
                //client.sendCommand(Command.HGET, BASE, DATABASE);
                //String redundancy = client.getBulkReply();
                //if(redundancy == null){
                //    System.out.printf("No Database Name %s\n", DATABASE);
                //}
                //else{
                //    StringBuilder key = new StringBuilder(DATABASE);
                //    key.append(redundancy);

                //    client.sendCommand(Command.HKEYS, SafeEncoder.encode(key.toString()));
                //    result_list = client.getMultiBulkReply();
                //    System.out.println(result_list);
                    //List<String> res = client.getMultiBulkReply();
                    //System.out.println(res);
                //}   

                //client.sendCommand(Command.SQLSHOWTABLE, database_name, table_name);
                //result = client.getBulkReply();
            }
            else if(op.equals("create")){

                ArrayList<String> parsed = parse.parseCreate(command);

                System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                System.out.println();
                System.out.printf("%s >> %s", "Table Name", parsed.get(0));
                System.out.println();
                System.out.printf("%s >> %s", "Attribute", parsed.get(1));
                System.out.println();
                System.out.printf("%s >> %s", "Type", parsed.get(2));
                System.out.println();

                //client.sendCommand(Command.SQLCREATETABLE, database_name, table_name, attributes, types);
                //result = client.getStatusCodeReply();
            }
            else if(op.equals("insert")){

                ArrayList<String> parsed = parse.parseInsert(command);

                System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                System.out.println();
                System.out.printf("%s >> %s", "Table Name", parsed.get(0));
                System.out.println();
                System.out.printf("%s >> %s", "Value", parsed.get(1));
                System.out.println();

                //client.sendCommand(Command.SQLINSERT, database_name, table_name, values);
                //result = client.getStatusCodeReply();
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

                System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                System.out.println();
                System.out.printf("%s >> %s", "Table", parsed.get(0));
                System.out.println();
                System.out.printf("%s >> %s", "Update", parsed.get(1));
                System.out.println();
                System.out.printf("%s >> %s", "Condition", parsed.get(2));
                System.out.println();

                //client.sendCommand(Command.SQLUPDATE, database_name, tables, updates, conditions);
                //result = client.getStatusCodeReply();

            }
            else if(op.equals("delete")){

                ArrayList<String> parsed = parse.parseDelete(command);

                System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                System.out.println();
                System.out.printf("%s >> %s", "Table", parsed.get(0));
                System.out.println();
                System.out.printf("%s >> %s", "Condition", parsed.get(1));
                System.out.println();

                //client.sendCommand(Command.SQLDELETE, database_name, tables, conditions);
                //result = client.getBulkReply();

            }
            else if(op.equals("exit")){
                break;
            }
        }

        client.close();

    }
}
