package dhsong;

import dhsong.Connection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;


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
        System.out.println("Data Key > " + keyDB);
        client.sendCommand(Command.HGET, SafeEncoder.encode(keyDB), SafeEncoder.encode(table));
        String ros_str;
        ros_str = client.getBulkReply();

        String key_table = table + ros_str;

        return key_table;
    }
    public static String getKeyAttribute(ClientConnection client, String base, String database, String table, String attribute){
        String keyTable = getKeyTable(client, base, database, table);

        System.out.println(keyTable);
        client.sendCommand(Command.HGET, SafeEncoder.encode(keyTable), SafeEncoder.encode(attribute));
        String ros_str;
        ros_str = client.getBulkReply();

        String value = ros_str;
        System.out.println("VAL is " + value);
        
        String key_attribute = attribute + value.split(",")[0];
        System.out.println(key_attribute);
        
        return key_attribute;
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

    public static void sqlDelete(ClientConnection client, String base, String database, String table, String conditions){
        Long ros_long;
        String rows = getRowIdxCondition(client, base, database, table, conditions);
        System.out.println(rows);
        List<String> rows_list = Arrays.asList(rows.split(","));
        Collections.sort(rows_list, Collections.reverseOrder());
        

        String keyDB = getKeyTable(client, base, database, table);
        List<String> attributes = new ArrayList<>();
        List<String> keyAttributes = new ArrayList<>();
        String row = new String();
        String lastData = new String();
        int lastRowId = -1;
        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyDB));
        attributes = client.getMultiBulkReply();

        for(int i = 0; i < attributes.size(); i++){
            keyAttributes.add(getKeyAttribute(client, base, database, table, attributes.get(i)));
        }
        
        for(int i = 0; i < rows_list.size(); i++){
            for(int j = 0; j < keyAttributes.size(); j++){
                client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode("ROW"));
                row = client.getBulkReply();
                lastRowId = Integer.parseInt(row) - 1;

                client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode(String.valueOf(lastRowId)));
                lastData = client.getBulkReply();

                if(rows_list.get(i).equals(String.valueOf(lastRowId))){
                    client.sendCommand(Command.HDEL, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode(String.valueOf(lastRowId)));
                    ros_long = client.getIntegerReply();
                    client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode("ROW"), SafeEncoder.encode(String.valueOf(lastRowId)));
                    ros_long = client.getIntegerReply();
                }
                else{
                    client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode(rows_list.get(i)), SafeEncoder.encode(lastData));
                    ros_long = client.getIntegerReply();
                    client.sendCommand(Command.HDEL, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode(String.valueOf(lastRowId)));
                    ros_long = client.getIntegerReply();
                    client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttributes.get(j)), SafeEncoder.encode("ROW"), SafeEncoder.encode(String.valueOf(lastRowId)));
                    ros_long = client.getIntegerReply();    
                }

            }
        }
    } 

    public static void sqlUpdate(ClientConnection client, String base, String database, String table, String updates, String conditions){
        Long ros_long;

        String rows = getRowIdxCondition(client, base, database, table, conditions);
        List<String> rows_list = Arrays.asList(rows.split(","));
        Collections.sort(rows_list, Collections.reverseOrder());

        String update_attribute = updates.split(" ")[0];
        String update_value = updates.split(" ")[1];

        System.out.println("UPDAT att" + update_attribute);
        System.out.println("UPDAT val" + update_value);
        
        String keyAttribute = getKeyAttribute(client, base, database, table, update_attribute);

        
        for(int i = 0; i < rows_list.size(); i++){
            client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(rows_list.get(i)), SafeEncoder.encode(update_value));
            ros_long = client.getIntegerReply();
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

    public static boolean isOp(String str){
        if(str.equals("&&") || str.equals("||") || str.equals("=") || str.equals(">=") || str.equals(">") || str.equals("<=") || str.equals("<")){
            return true;
        }
        else{
            return false;
        }
    }
    public static String operateEqual(ClientConnection client, String base, String database, String table, String attribute, String value){
        String ros_str;
        List<String> ros_list = new ArrayList<>();
        List<String> rows = new ArrayList<>();


        StringBuilder result = new StringBuilder();
        String keyAttribute = getKeyAttribute(client, base, database, table, attribute);

        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyAttribute));
        ros_list = client.getMultiBulkReply();
        rows = ros_list;
        rows.remove("ROW");

        for(int i = 0; i < rows.size(); i++){
            client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(rows.get(i)));
            ros_str = client.getBulkReply();
            if(ros_str.equals(value)){
                if(result.length() == 0){
                    result.append(rows.get(i));
                }
                else{
                    result.append("," + rows.get(i));
                }
            }
        }

        return result.toString();
    }
    public static String operateLess(ClientConnection client, String base, String database, String table, String attribute, String value, boolean eq){
        String ros_str;
        List<String> ros_list = new ArrayList<>();
        List<String> rows = new ArrayList<>();

        StringBuilder result = new StringBuilder();

        String keyAttribute = getKeyAttribute(client, base, database, table, attribute);

        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyAttribute));
        ros_list = client.getMultiBulkReply();
        rows = ros_list;
        rows.remove("ROW");

        for(int i = 0; i < rows.size(); i++){
            client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(rows.get(i)));
            ros_str = client.getBulkReply();
            if(eq){
                if(ros_str.compareTo(value) <= 0){
                    if(result.length() == 0){
                        result.append(rows.get(i));
                    }
                    else{
                        result.append("," + rows.get(i));
                    }
                }
            }
            else{
                if(ros_str.compareTo(value) < 0){
                    if(result.length() == 0){
                        result.append(rows.get(i));
                    }
                    else{
                        result.append("," + rows.get(i));
                    }
                }
            }
        }

        return result.toString();
    }
    public static String operateGreat(ClientConnection client, String base, String database, String table, String attribute, String value, boolean eq){
        String ros_str;
        List<String> ros_list = new ArrayList<>();
        List<String> rows = new ArrayList<>();

        StringBuilder result = new StringBuilder();

        String keyAttribute = getKeyAttribute(client, base, database, table, attribute);

        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyAttribute));
        ros_list = client.getMultiBulkReply();
        rows = ros_list;
        rows.remove("ROW");

        for(int i = 0; i < rows.size(); i++){
            client.sendCommand(Command.HGET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(rows.get(i)));
            ros_str = client.getBulkReply();
            if(eq){
                if(ros_str.compareTo(value) >= 0){
                    if(result.length() == 0){
                        result.append(rows.get(i));
                    }
                    else{
                        result.append("," + rows.get(i));
                    }
                }
            }
            else{
                if(ros_str.compareTo(value) > 0){
                    if(result.length() == 0){
                        result.append(rows.get(i));
                    }
                    else{
                        result.append("," + rows.get(i));
                    }
                }
            }
        }

        return result.toString();    
    }


    public static String getRowIdxCondition(ClientConnection client, String base, String database, String table, String conditions){
        StringBuilder result = new StringBuilder();
        String[] operations = conditions.split(" ");
        Stack<String> st = new Stack<>();
        
        //System.out.println(conditions);
        //for(int s = 0; s < operations.length; s++){
        //    System.out.println(operations[s]);
        //}

        int idx = 0;
        String c = new String();
        String a = new String();
        String b = new String();
        StringBuilder c_buf = new StringBuilder();
        List<String> a_list = new ArrayList<>();
        List<String> b_list = new ArrayList<>();
        List<String> c_list = new ArrayList<>();

        while(idx < operations.length){
            if(isOp(operations[idx])){
                b = st.pop();
                a = st.pop();
                //System.out.println(a + "\n" + b);
                if(operations[idx].equals("=")){
                    c = operateEqual(client, base, database, table, a, b);
                    st.push(c);
                }
                else if(operations[idx].equals(">=")){
                    c = operateGreat(client, base, database, table, a, b, true);
                    st.push(c);
                }
                else if(operations[idx].equals("<=")){
                    c = operateLess(client, base, database, table, a, b, true);
                    st.push(c);
                }
                else if(operations[idx].equals(">")){
                    c = operateGreat(client, base, database, table, a, b, false);
                    st.push(c);
                }
                else if(operations[idx].equals("<")){
                    c = operateLess(client, base, database, table, a, b, false);
                    st.push(c);
                }                
                else if(operations[idx].equals("&&")){
                    a_list = Arrays.asList(a.split(","));
                    b_list = Arrays.asList(b.split(","));

                    for(int i = 0; i < a_list.size(); i++){
                        if(b_list.contains(a_list.get(i))){
                            if(c_buf.length() == 0){
                                c_buf.append(a_list.get(i));
                            }
                            else{
                                c_buf.append("," + a_list.get(i));
                            }
                        }
                    }
                    c = c_buf.toString();
                    c_buf.delete(0, c_buf.length());
                    st.push(c);
                }
                else if(operations[idx].equals("||")){
                    a_list = Arrays.asList(a.split(","));
                    b_list = Arrays.asList(b.split(","));
                    System.out.println("list A " + a_list);
                    System.out.println("list B " + b_list);

                    for(int i = 0; i < a_list.size(); i++){
                        if(!c_list.contains(a_list.get(i))){
                            c_list.add(a_list.get(i));
                            if(c_buf.length() == 0){
                                c_buf.append(a_list.get(i));
                            }
                            else{
                                c_buf.append("," + a_list.get(i));
                            }
                        }
                    }
                    for(int i = 0; i < b_list.size(); i++){
                        if(!c_list.contains(b_list.get(i))){
                            c_list.add(b_list.get(i));
                            if(c_buf.length() == 0){
                                c_buf.append(b_list.get(i));
                            }
                            else{
                                c_buf.append("," + b_list.get(i));
                            }
                        }
                    }

                    c = c_buf.toString();
                    c_buf.delete(0, c_buf.length());
                    st.push(c);
                }
            }
            else{
                st.push(operations[idx]);
            }
            idx++;
        }

        return st.pop();
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
        System.out.println(ros_list);

        if(ros_list.isEmpty()){
            setup(client, BASE, DATABASE);
        }
    
        while(true){
            System.out.println("Enter the MySQL Command Below: ");
            System.out.print("[MySQL-to-Redis]  ");
            String command = sc.nextLine();
            String[] command_split = command.split(" ");
            command_split[0] = command_split[0].toLowerCase();
            String op = command_split[0];
            
            
            if(op.equals("show")){
                
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

                sqlUpdate(client, BASE, DATABASE, parsed.get(0), parsed.get(1), parsed.get(2));
            }
            else if(op.equals("delete")){

                ArrayList<String> parsed = parse.parseDelete(command);
 
                //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                //System.out.println();
                //System.out.printf("%s >> %s", "Table", parsed.get(0));
                //System.out.println();
                //System.out.printf("%s >> %s", "Condition", parsed.get(1));
                //System.out.println();

                sqlDelete(client, BASE, DATABASE, parsed.get(0), parsed.get(1));
            }
            else if(op.equals("exit")){
                break;
            }
        }

        client.close();

    }
}
