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
    public static String getRedundancy(ClientConnection client, String a){
        String ros_str;
        int red = 0;
        client.sendCommand(Command.GET, SafeEncoder.encode(a));
        ros_str = client.getBulkReply();
        if(ros_str != null){
            red = Integer.parseInt(ros_str) + 1; 
        }

        return String.valueOf(red);
    }

    public static String getKeyDB(ClientConnection client, String base, String database){
        client.sendCommand(Command.HGET, SafeEncoder.encode(base), SafeEncoder.encode(database));
        String ros_str;
        ros_str = client.getBulkReply();

        String key_database = database + ros_str;

        return key_database;
    }
    public static String getKeyTable(ClientConnection client, String base, String database, String table){
        String keyDB = getKeyDB(client, base, database);
        client.sendCommand(Command.HGET, SafeEncoder.encode(keyDB), SafeEncoder.encode(table));
        String ros_str;
        ros_str = client.getBulkReply();

        String key_table = table + ros_str;

        return key_table;
    }


    public static void sqlInsert(ClientConnection client, String base, String database, String table, String values){
        Long ros_long;
        String ros_str;
        List<String> ros_list = new ArrayList<>();
        List<String> attributes = new ArrayList<>();

        String keyTable = getKeyTable(client, base, database, table);

        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyTable));
        ros_list = client.getMultiBulkReply();
        attributes = ros_list;

        String[] value_list = values.split(",");
        String[] row = null;

        int idx = 0;
        while(idx < attributes.size()){
            client.sendCommand(Command.HGET, SafeEncoder.encode(keyTable), SafeEncoder.encode(attributes.get(idx)));
            ros_str = client.getBulkReply();
            String[] red_id_type = ros_str.split(",");
            int attribute_id = Integer.parseInt(red_id_type[1]);
            String redundancyAttribute = red_id_type[0];
            String keyAttribute = attributes.get(idx) + redundancyAttribute;


            client.sendCommand(Command.KEYS, SafeEncoder.encode(keyAttribute));
            ros_list = client.getMultiBulkReply();
            if(ros_list.isEmpty()){

                client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode("0"), SafeEncoder.encode(value_list[attribute_id]));                
                ros_long = client.getIntegerReply();

                client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode("ROW"), SafeEncoder.encode("1"));                
                ros_long = client.getIntegerReply();
                
            }
            else{
                client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode("ROW"));                
                ros_str = client.getBulkReply();
                row = ros_str.split(",");

                client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(row[0]), SafeEncoder.encode(value_list[attribute_id]));                
                ros_long = client.getIntegerReply();

                String newRow;
                if(row.length == 1){
                    newRow = String.valueOf(Integer.parseInt(row[0]) + 1);
                    client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode("ROW"), SafeEncoder.encode(newRow));                
                    ros_long = client.getIntegerReply();
    
                }
                else{
                    StringBuilder newRowList = new StringBuilder(row[1]);
                    for(int i = 2; i < row.length; i++){
                        newRowList.append("," + row[i]);
                    }
                    client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode("ROW"), SafeEncoder.encode(newRowList.toString()));                
                    ros_long = client.getIntegerReply();
                }

            }
            idx += 1;
    
        }
    } 

    public static void sqlCreate(ClientConnection client, String base, String database, String table, String attibutes, String types){
        Long ros_long;
        String ros_str;

        String keyDB = getKeyDB(client, base, database);
        String redundancyTable = getRedundancy(client, table);
        client.sendCommand(Command.HSET, SafeEncoder.encode(keyDB), SafeEncoder.encode(table), SafeEncoder.encode(redundancyTable));
        ros_long = client.getIntegerReply();
        client.sendCommand(Command.SET, SafeEncoder.encode(database), SafeEncoder.encode(redundancyTable));
        ros_str = client.getStatusCodeReply();

        String keyTable = table + redundancyTable;
        String[] attributes_list = attibutes.split(",");
        String[] types_list = types.split(",");

        String value;
        for(int idx = 0; idx < attributes_list.length; idx++){
            String redundancyAttribute = getRedundancy(client, attributes_list[idx]);
            client.sendCommand(Command.SET, SafeEncoder.encode(attributes_list[idx]), SafeEncoder.encode(redundancyAttribute));
            ros_str = client.getStatusCodeReply();

            value = redundancyAttribute + "," + String.valueOf(idx) + "," + types_list[idx].substring(0, 1);
            client.sendCommand(Command.HSET, SafeEncoder.encode(keyTable), SafeEncoder.encode(attributes_list[idx]), SafeEncoder.encode(value));
            ros_long = client.getIntegerReply();
        }
    } 

    public static void sqlShow(ClientConnection client, String base, String database){
        client.sendCommand(Command.HGET, SafeEncoder.encode(base), SafeEncoder.encode(database));
        String ros_str;
        ros_str = client.getBulkReply();

        String key = database + ros_str;

        client.sendCommand(Command.HKEYS, SafeEncoder.encode(key));
        
        List<String> ros_list = new ArrayList<>();
        ros_list = client.getMultiBulkReply();
        
        if(ros_list.isEmpty()){
            System.out.println("No Table at DB");
        }
        else{
            for(int i = 0; i < ros_list.size(); i++){
                System.out.println(ros_list.get(i));
            }
        }

    } 

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
            System.out.println("Enter the MySQL Command Below: ");
            System.out.print("[MySQL-to-Redis]");
            String command = sc.nextLine();
            String[] command_split = command.split(" ");
            command_split[0] = command_split[0].toLowerCase();
            String op = command_split[0];
            
            
            if(op.equals("show")){
                
                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                sqlShow(client, BASE, DATABASE);
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

                sqlCreate(client, BASE, DATABASE, parsed.get(0), parsed.get(1), parsed.get(2));

                //client.sendCommand(Command.SQLCREATETABLE, database_name, table_name, attributes, types);
                //result = client.getStatusCodeReply();
            }
            else if(op.equals("insert")){

                ArrayList<String> parsed = parse.parseInsert(command);

                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table Name", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Value", parsed.get(1));
                //System.out.println();

                sqlInsert(client, BASE, DATABASE, parsed.get(0), parsed.get(1));
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
