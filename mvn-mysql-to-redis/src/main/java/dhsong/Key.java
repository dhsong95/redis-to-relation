package dhsong;

import dhsong.ClientConnection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;

public class Key{
    public static String getKeyDB(ClientConnection client, String base, String database){
        client.sendCommand(Command.HGET, SafeEncoder.encode(base), SafeEncoder.encode(database));
        String ros_str;
        ros_str = client.getBulkReply();
    
        String key_database = database + ros_str;
    
        return key_database;
    }
    public static String getKeyTable(ClientConnection client, String base, String database, String table){
        String keyDB = getKeyDB(client, base, database);
        //System.out.println("Data Key > " + keyDB);
        client.sendCommand(Command.HGET, SafeEncoder.encode(keyDB), SafeEncoder.encode(table));
        String ros_str;
        ros_str = client.getBulkReply();
    
        String key_table = table + ros_str;
    
        return key_table;
    }
    public static String getKeyAttribute(ClientConnection client, String base, String database, String table, String attribute){
        String keyTable = getKeyTable(client, base, database, table);
    
        //System.out.println(keyTable);
        client.sendCommand(Command.HGET, SafeEncoder.encode(keyTable), SafeEncoder.encode(attribute));
        String ros_str;
        ros_str = client.getBulkReply();
    
        String value = ros_str;
        //System.out.println("VAL is " + value);
        
        String key_attribute = attribute + value.split(",")[0];
        //System.out.println(key_attribute);
        
        return key_attribute;
    }    
}

