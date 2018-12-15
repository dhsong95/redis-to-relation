package dhsong;

import dhsong.ClientConnection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class Util{
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
                    //System.out.println("list A " + a_list);
                    //System.out.println("list B " + b_list);

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
        String keyAttribute = Key.getKeyAttribute(client, base, database, table, attribute);

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

        String keyAttribute = Key.getKeyAttribute(client, base, database, table, attribute);

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

        String keyAttribute = Key.getKeyAttribute(client, base, database, table, attribute);

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

}