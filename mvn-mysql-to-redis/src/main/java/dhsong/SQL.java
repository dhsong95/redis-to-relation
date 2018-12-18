package dhsong;

import dhsong.ClientConnection;
import dhsong.Key;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import dhsong.Util;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SQL{

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

    public static void sqlCreate(ClientConnection client, String base, String database, String table, String attibutes, String types){
        Long ros_long;
        String ros_str;

        String keyDB = Key.getKeyDB(client, base, database);
        String redundancyTable = Util.getRedundancy(client, table);
        client.sendCommand(Command.HSET, SafeEncoder.encode(keyDB), SafeEncoder.encode(table), SafeEncoder.encode(redundancyTable));
        ros_long = client.getIntegerReply();
        client.sendCommand(Command.SET, SafeEncoder.encode(database), SafeEncoder.encode(redundancyTable));
        ros_str = client.getStatusCodeReply();

        String keyTable = table + redundancyTable;
        String[] attributes_list = attibutes.split(",");
        String[] types_list = types.split(",");

        String value;
        for(int idx = 0; idx < attributes_list.length; idx++){
            String redundancyAttribute = Util.getRedundancy(client, attributes_list[idx]);
            client.sendCommand(Command.SET, SafeEncoder.encode(attributes_list[idx]), SafeEncoder.encode(redundancyAttribute));
            ros_str = client.getStatusCodeReply();

            value = redundancyAttribute + "," + String.valueOf(idx) + "," + types_list[idx].substring(0, 1);
            client.sendCommand(Command.HSET, SafeEncoder.encode(keyTable), SafeEncoder.encode(attributes_list[idx]), SafeEncoder.encode(value));
            ros_long = client.getIntegerReply();
        }
    } 

    
    public static void sqlInsert(ClientConnection client, String base, String database, String table, String values){
        Long ros_long;
        String ros_str;
        List<String> ros_list = new ArrayList<>();
        List<String> attributes = new ArrayList<>();

        String keyTable = Key.getKeyTable(client, base, database, table);

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

    public static void sqlUpdate(ClientConnection client, String base, String database, String table, String updates, String conditions){
        Long ros_long;

        String rows = Util.getRowIdxCondition(client, base, database, table, conditions);
        List<String> rows_list = Arrays.asList(rows.split(","));
        Collections.sort(rows_list, Collections.reverseOrder());

        String update_attribute = updates.split(" ")[0];
        String update_value = updates.split(" ")[1];
        
        String keyAttribute = Key.getKeyAttribute(client, base, database, table, update_attribute);

        if(rows_list.size() == 1 && !Util.isStringInteger(rows_list.get(0))){
            for(int i = 0; i < rows_list.size(); i++){
                client.sendCommand(Command.HSET, SafeEncoder.encode(keyAttribute), SafeEncoder.encode(rows_list.get(i)), SafeEncoder.encode(update_value));
                ros_long = client.getIntegerReply();
            }
        }

        
    } 



    public static void sqlDelete(ClientConnection client, String base, String database, String table, String conditions){
        Long ros_long;
        String rows = Util.getRowIdxCondition(client, base, database, table, conditions);
        //System.out.println(rows);
        List<String> rows_list = Arrays.asList(rows.split(","));
        Collections.sort(rows_list, Collections.reverseOrder());
        

        String keyDB = Key.getKeyTable(client, base, database, table);
        List<String> attributes = new ArrayList<>();
        List<String> keyAttributes = new ArrayList<>();
        String row = new String();
        String lastData = new String();
        int lastRowId = -1;
        client.sendCommand(Command.HKEYS, SafeEncoder.encode(keyDB));
        attributes = client.getMultiBulkReply();

        for(int i = 0; i < attributes.size(); i++){
            keyAttributes.add(Key.getKeyAttribute(client, base, database, table, attributes.get(i)));
        }


        if(!(rows_list.size() == 1 && !Util.isStringInteger(rows_list.get(0)))){
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
    } 


}