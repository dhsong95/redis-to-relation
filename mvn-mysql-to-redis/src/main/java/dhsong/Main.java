package dhsong;

import dhsong.Connection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.Scanner;
//import redis.clients.jedis.commands.ProtocolCommand;


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
    public static String makePostfix(String op){
        System.out.println(op);
        StringBuilder result = new StringBuilder();
        String[] operations = op.split(" ");
        Stack<String> stack = new Stack<>();
        String pop = new String();

        int idx = 0;
        while(idx < operations.length){
            if(operations[idx].equals("(")){
                stack.push(operations[idx]);
                idx += 1;
            }           
            else if(operations[idx].equals(")")){
                while(true){
                    pop = stack.peek();
                    if(!stack.isEmpty() && pop.equals("(")){
                        stack.pop();
                        break;
                    }
                    else{
                        if(result.length() == 0){
                            result.append(stack.pop());
                        }
                        else{
                            result.append(" " + stack.pop());
                        }        
                    }
                }

                idx += 1;
            }
            else if(operations[idx].equals("&&") || operations[idx].equals("||")){
                pop = stack.peek();
                if(pop.equals("<=") || pop.equals(">=") || pop.equals("<") || pop.equals(">") || pop.equals("(")){
                    stack.push(operations[idx]);
                }
                else if(pop.equals("&&") || pop.equals("||")){
                    if(result.length() == 0){
                        result.append(stack.pop());
                    }
                    else{
                        result.append(" " + stack.pop());
                    }
                    stack.push(operations[idx]);
                }
                idx += 1;
            }
            else if(operations[idx].equals("<=") || operations[idx].equals(">=") || operations[idx].equals("<") || operations[idx].equals(">") || operations[idx].equals("=")){
                pop = stack.peek();
                
                while(stack.isEmpty() && !(pop.equals("&&") || pop.equals("||"))){
                    if(result.length() == 0){
                        result.append(stack.pop());
                    }
                    else{
                        result.append(" " + stack.pop());
                    }
                    pop = stack.peek();
                }
                stack.push(operations[idx]);
                idx += 1;
            }
            else{
                if(result.length() == 0){
                    result.append(operations[idx]);
                }
                else{
                    result.append(" " + operations[idx]);
                }
                idx += 1;
            }
        }

        while(!stack.isEmpty()){
            if(result.length() == 0){
                result.append(stack.pop());
            }
            else{
                result.append(" " + stack.pop());
            }
        }

        return result.toString();

    } 
    public static void main( String[] args )
    {   
        Scanner sc = new Scanner(System.in);
        String BASE = "MySQL";
        String database_name = "database";
        ClientConnection client = new ClientConnection();
        
        while(true){
            System.out.println("Enter the MySQL Command Below: ");
            System.out.print("[MySQL-to-Redis]");
            String command = sc.nextLine();
            String[] command_split = command.split(" ");
            command_split[0] = command_split[0].toLowerCase();
            String op = command_split[0];
            
            String result;
            
            if(op.equals("show")){
                database_name = command_split[1].substring(0, command_split[1].length()-1);
                
                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                client.sendCommand(Command.HGET, BASE, database_name);
                String redundancy = client.getBulkReply();
                if(redundancy == null){
                    System.out.println("Database is not created");
                }
                else{
                    System.out.println("DEBUG");
                    StringBuilder key = new StringBuilder(database_name);
                    key.append(redundancy);
                    System.out.println(key); 
                    client.sendCommand(Command.HKEYS, SafeEncoder.encode(key.toString()));
                    List<String> temp = new ArrayList<>();
                    temp = client.getMultiBulkReply();
                    System.out.println(temp);
                    //List<String> res = client.getMultiBulkReply();
                    //System.out.println(res);
                }   

                //client.sendCommand(Command.SQLSHOWTABLE, database_name, table_name);
                //result = client.getBulkReply();
            }

            else if(op.equals("create")){
                String[] temp = command.split("[(]");
                String[] table_temp = temp[0].split(" ");
                String table_name = table_temp[table_temp.length - 1];

                String[] attribute_type = temp[1].split(",");
                StringBuilder attributes = new StringBuilder();
                StringBuilder types = new StringBuilder();

                for(int i=0; i<attribute_type.length; i++){
                    
                    if(attribute_type[i].charAt(0) == ' '){
                        attribute_type[i] = attribute_type[i].substring(1, attribute_type[i].length());
                    }
                    
                    if(attribute_type[i].charAt(attribute_type[i].length()-1) == ';'){
                        attribute_type[i] = attribute_type[i].substring(0, attribute_type[i].length() - 2);
                    }

                    String[] tmp = attribute_type[i].split(" ");
             
                    if(i == 0){
                        attributes.append(tmp[0]);
                        types.append(tmp[1].toUpperCase());
                    }
                    else{
                        attributes.append(","+tmp[0]);
                        types.append(","+tmp[1].toUpperCase());
                    }
                }

                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                System.out.printf("%s >> %s", "Table Name", table_name);
                System.out.println();
                System.out.printf("%s >> %s", "Attribute", attributes);
                System.out.println();
                System.out.printf("%s >> %s", "Type", types);
                System.out.println();

                //client.sendCommand(Command.SQLCREATETABLE, database_name, table_name, attributes, types);
                //result = client.getStatusCodeReply();
            }
            else if(op.equals("insert")){
                String[] temp = command.split("[(]");
                String[] table_temp = temp[0].split(" ");
                String table_name = table_temp[table_temp.length - 2];

                String[] value = temp[1].split(",");
                StringBuilder values = new StringBuilder();

                for(int i=0; i<value.length; i++){
                    if(value[i].charAt(0) == ' '){
                        value[i] = value[i].substring(1, value[i].length());
                    }
                    if(value[i].charAt(value[i].length()-1) == ';'){
                        value[i] = value[i].substring(0, value[i].length() - 2);
                    }

                    if(i == 0){
                        values.append(value[i]);
                    }
                    else{
                        values.append(","+value[i]);
                    }

                }

                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                System.out.printf("%s >> %s", "Table Name", table_name);
                System.out.println();
                System.out.printf("%s >> %s", "Value", values);
                System.out.println();

                //client.sendCommand(Command.SQLINSERT, database_name, table_name, values);
                //result = client.getStatusCodeReply();
            }
            else if(op.equals("select")){
                String[] temp = command.split(" ");
                int idx_select = -1;
                int idx_from = -1;
                int idx_where = -1;

                for(int i = 0; i < temp.length; i++){
                    if(temp[i].toLowerCase().equals("select")){
                        idx_select = i;
                    }
                    else if(temp[i].toLowerCase().equals("from")){
                        idx_from = i;
                    }
                    else if(temp[i].toLowerCase().equals("where")){
                        idx_where = i;
                    }
                }
                if(idx_where == -1){
                    idx_where = temp.length;
                }

                StringBuilder attributes = new StringBuilder();
                for(int i = idx_select + 1; i < idx_from; i++){
                    if(temp[i].charAt(temp[i].length() - 1) == ','){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                                        
                    if(i == idx_select + 1){
                        attributes.append(temp[i]);
                    }
                    else{
                        attributes.append(","+temp[i]);                        
                    }
                }

                StringBuilder tables = new StringBuilder();
                for(int i = idx_from + 1; i < idx_where; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ','){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    if(temp[i].charAt(temp[i].length() - 1) == ';'){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    
                    if(i == idx_from + 1){
                        tables.append(temp[i]);
                    }
                    else{
                        tables.append(","+temp[i]);                        
                    }
                }

                StringBuilder conditions = new StringBuilder();
                for(int i = idx_where + 1; i < temp.length; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ';'){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    

                    int idx = 0;
                    StringBuilder sb = new StringBuilder();

                    while(idx < temp[i].length()){

                        if(temp[i].length() == 1){
                            if(conditions.length() == 0){
                                conditions.append(temp[i]);
                            }
                            else{
                                conditions.append(" " + temp[i]);
                            }
                            idx += 1;
                        }
                        else{
                            if(temp[i].substring(idx, idx + 1).equals("(")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("(");
                                    }
                                    else{
                                        conditions.append(sb + " (");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" (");
                                    }
                                    else{
                                        conditions.append(" " + sb + " (");
                                    }
                                }
                                idx += 1;                            
                                sb.delete(0, sb.length());
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(")")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append(")");
                                    }
                                    else{
                                        conditions.append(sb + " )");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" )");
                                    }
                                    else{
                                        conditions.append(" " + sb + " )");
                                    }
                                }
                                sb.delete(0, sb.length());
                                idx += 1;         
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("=")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("=");
                                    }
                                    else{
                                        conditions.append(sb + " =");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" =");
                                    }
                                    else{
                                        conditions.append(" " + sb + " =");
                                    }
                                }
                                idx += 1;
                                sb.delete(0, sb.length());                                                       
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("<")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("<=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<=");
                                        }
                                        else{
                                            conditions.append(sb + " <=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <=");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <=");
                                        }
                                    }
                                    idx += 2;
                                    sb.delete(0, sb.length());                                                                                               
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<");
                                        }
                                        else{
                                            conditions.append(sb + " <");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(">")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals(">=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(sb + " >=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(" " + sb + " >=");
                                        }
                                    }
                                    idx += 2;                           
                                    sb.delete(0, sb.length());    
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">");
                                        }
                                        else{
                                            conditions.append(sb + " >");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >");
                                        }
                                        else{
                                            conditions.append(" " + sb + " >");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("&&")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("&&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " &&");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" &&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " &&");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("||")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " ||");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" ||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " ||");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else{
                                if(idx == 0){
                                    if(temp[i].length() >= 3 && temp[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                        sb.append("&&");
                                        idx += 3;
                                    }
                                    else if(temp[i].length() >= 2 && temp[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                        sb.append("||");
                                        idx += 2;
                                    }
                                    else{
                                        sb.append(temp[i].substring(idx, idx + 1));
                                        idx += 1;                                        
                                    }
                                }
                                else{
                                    sb.append(temp[i].substring(idx, idx + 1));
                                    idx += 1;                                    
                                }

                            }
                        }
                    }
                    if(sb.length() != 0){
                        if(conditions.length() != 0){
                            conditions.append(" " + sb.toString());
                            sb.delete(0, sb.length());
                        }
                        else{
                            conditions.append(sb.toString());
                            sb.delete(0, sb.length());
                        }
                    }
                }

                String conditions_postfix = makePostfix("( " + conditions.toString() + " )");
                
                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                System.out.printf("%s >> %s", "Table", tables);
                System.out.println();
                System.out.printf("%s >> %s", "Attribute", attributes);
                System.out.println();
                System.out.printf("%s >> %s", "Condition", conditions);
                System.out.println();
                System.out.printf("%s >> %s", "Prefix Condition", conditions_postfix);
                System.out.println();
                //client.sendCommand(Command.SQLSELECT, database_name, tables, attributes, conditions);
                //result = client.getBulkReply();
            }
            else if(op.equals("update")){
                String[] temp = command.split(" ");
                int idx_update = -1;
                int idx_set = -1;
                int idx_where = -1;

                for(int i = 0; i < temp.length; i++){
                    if(temp[i].toLowerCase().equals("update")){
                        idx_update = i;
                    }
                    else if(temp[i].toLowerCase().equals("set")){
                        idx_set = i;
                    }
                    else if(temp[i].toLowerCase().equals("where")){
                        idx_where = i;
                    }
                }
                if(idx_where == -1){
                    idx_where = temp.length;
                }


                StringBuilder tables = new StringBuilder();
                for(int i = idx_update + 1; i < idx_set; i++){
                    if(temp[i].charAt(temp[i].length() - 1) == ','){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    
                    if(i == idx_update + 1){
                        tables.append(temp[i]);
                    }
                    else{
                        tables.append(","+temp[i]);                        
                    }
                }

                StringBuilder updates = new StringBuilder();
                for(int i = idx_set + 1; i < idx_where; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ','){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }

                    if(i == idx_set + 1){
                        updates.append(temp[i]);
                    }
                    else{
                        updates.append(" "+temp[i]);                        
                    }
                    
                }

                StringBuilder conditions = new StringBuilder();
                for(int i = idx_where + 1; i < temp.length; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ';'){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    

                    int idx = 0;
                    StringBuilder sb = new StringBuilder();

                    while(idx < temp[i].length()){

                        if(temp[i].length() == 1){
                            if(conditions.length() == 0){
                                conditions.append(temp[i]);
                            }
                            else{
                                conditions.append(" " + temp[i]);
                            }
                            idx += 1;
                        }
                        else{
                            if(temp[i].substring(idx, idx + 1).equals("(")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("(");
                                    }
                                    else{
                                        conditions.append(sb + " (");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" (");
                                    }
                                    else{
                                        conditions.append(" " + sb + " (");
                                    }
                                }
                                idx += 1;                            
                                sb.delete(0, sb.length());
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(")")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append(")");
                                    }
                                    else{
                                        conditions.append(sb + " )");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" )");
                                    }
                                    else{
                                        conditions.append(" " + sb + " )");
                                    }
                                }
                                sb.delete(0, sb.length());
                                idx += 1;         
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("=")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("=");
                                    }
                                    else{
                                        conditions.append(sb + " =");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" =");
                                    }
                                    else{
                                        conditions.append(" " + sb + " =");
                                    }
                                }
                                idx += 1;
                                sb.delete(0, sb.length());                                                       
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("<")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("<=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<=");
                                        }
                                        else{
                                            conditions.append(sb + " <=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <=");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <=");
                                        }
                                    }
                                    idx += 2;
                                    sb.delete(0, sb.length());                                                                                               
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<");
                                        }
                                        else{
                                            conditions.append(sb + " <");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(">")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals(">=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(sb + " >=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(" " + sb + " >=");
                                        }
                                    }
                                    idx += 2;                           
                                    sb.delete(0, sb.length());    
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">");
                                        }
                                        else{
                                            conditions.append(sb + " >");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >");
                                        }
                                        else{
                                            conditions.append(" " + sb + " >");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("&&")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("&&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " &&");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" &&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " &&");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("||")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " ||");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" ||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " ||");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else{
                                if(idx == 0){
                                    if(temp[i].length() >= 3 && temp[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                        sb.append("&&");
                                        idx += 3;
                                    }
                                    else if(temp[i].length() >= 2 && temp[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                        sb.append("||");
                                        idx += 2;
                                    }
                                    else{
                                        sb.append(temp[i].substring(idx, idx + 1));
                                        idx += 1;                                        
                                    }
                                }
                                else{
                                    sb.append(temp[i].substring(idx, idx + 1));
                                    idx += 1;                                    
                                }

                            }
                        }
                    }
                    if(sb.length() != 0){
                        if(conditions.length() != 0){
                            conditions.append(" " + sb.toString());
                            sb.delete(0, sb.length());
                        }
                        else{
                            conditions.append(sb.toString());
                            sb.delete(0, sb.length());
                        }
                    }
                }


                String conditions_postfix = makePostfix("( " + conditions.toString() + " )");

                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                System.out.printf("%s >> %s", "Table", tables);
                System.out.println();
                System.out.printf("%s >> %s", "Update", updates);
                System.out.println();
                System.out.printf("%s >> %s", "Condition", conditions);
                System.out.println();
                System.out.printf("%s >> %s", "Prefix Condition", conditions_postfix);
                System.out.println();

                //client.sendCommand(Command.SQLUPDATE, database_name, tables, updates, conditions);
                //result = client.getStatusCodeReply();

            }
            else if(op.equals("delete")){
                String[] temp = command.split(" ");
                int idx_from = -1;
                int idx_where = -1;

                for(int i = 0; i < temp.length; i++){
                    if(temp[i].toLowerCase().equals("from")){
                        idx_from = i;
                    }
                    else if(temp[i].toLowerCase().equals("where")){
                        idx_where = i;
                    }
                }
                if(idx_where == -1){
                    idx_where = temp.length;
                }

                StringBuilder tables = new StringBuilder();
                for(int i = idx_from + 1; i < idx_where; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ','){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    
                    if(i == idx_from + 1){
                        tables.append(temp[i]);
                    }
                    else{
                        tables.append(","+temp[i]);                        
                    }
                }

                StringBuilder conditions = new StringBuilder();
                for(int i = idx_where + 1; i < temp.length; i++){
                    if(temp[i].charAt(temp[i].length()-1) == ';'){
                        temp[i] = temp[i].substring(0, temp[i].length() - 1);
                    }
                    

                    int idx = 0;
                    StringBuilder sb = new StringBuilder();

                    while(idx < temp[i].length()){

                        if(temp[i].length() == 1){
                            if(conditions.length() == 0){
                                conditions.append(temp[i]);
                            }
                            else{
                                conditions.append(" " + temp[i]);
                            }
                            idx += 1;
                        }
                        else{
                            if(temp[i].substring(idx, idx + 1).equals("(")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("(");
                                    }
                                    else{
                                        conditions.append(sb + " (");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" (");
                                    }
                                    else{
                                        conditions.append(" " + sb + " (");
                                    }
                                }
                                idx += 1;                            
                                sb.delete(0, sb.length());
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(")")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append(")");
                                    }
                                    else{
                                        conditions.append(sb + " )");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" )");
                                    }
                                    else{
                                        conditions.append(" " + sb + " )");
                                    }
                                }
                                sb.delete(0, sb.length());
                                idx += 1;         
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("=")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("=");
                                    }
                                    else{
                                        conditions.append(sb + " =");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" =");
                                    }
                                    else{
                                        conditions.append(" " + sb + " =");
                                    }
                                }
                                idx += 1;
                                sb.delete(0, sb.length());                                                       
                            }
                            else if(temp[i].substring(idx, idx + 1).equals("<")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("<=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<=");
                                        }
                                        else{
                                            conditions.append(sb + " <=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <=");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <=");
                                        }
                                    }
                                    idx += 2;
                                    sb.delete(0, sb.length());                                                                                               
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append("<");
                                        }
                                        else{
                                            conditions.append(sb + " <");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" <");
                                        }
                                        else{
                                            conditions.append(" " + sb + " <");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(temp[i].substring(idx, idx + 1).equals(">")){
                                if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals(">=")){
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(sb + " >=");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >=");
                                        }
                                        else{
                                            System.out.println(sb);
                                            conditions.append(" " + sb + " >=");
                                        }
                                    }
                                    idx += 2;                           
                                    sb.delete(0, sb.length());    
                                }
                                else{
                                    if(conditions.length() == 0){
                                        if(sb.length() == 0){
                                            conditions.append(">");
                                        }
                                        else{
                                            conditions.append(sb + " >");
                                        }
                                    }
                                    else{
                                        if(sb.length() == 0){
                                            conditions.append(" >");
                                        }
                                        else{
                                            conditions.append(" " + sb + " >");
                                        }
                                    }
                                    idx += 1;
                                    sb.delete(0, sb.length());                                                           
                                }
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("&&")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("&&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " &&");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" &&");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " &&");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else if(idx <= temp[i].length() - 2 && temp[i].substring(idx, idx + 2).equals("||")){
                                if(conditions.length() == 0){
                                    if(sb.length() == 0){
                                        conditions.append("||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(sb + " ||");
                                    }
                                }
                                else{
                                    if(sb.length() == 0){
                                        conditions.append(" ||");
                                    }
                                    else{
                                        System.out.println(sb);
                                        conditions.append(" " + sb + " ||");
                                    }
                                }
                                idx += 2;                           
                                sb.delete(0, sb.length());    
                            }
                            else{
                                if(idx == 0){
                                    if(temp[i].length() >= 3 && temp[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                        sb.append("&&");
                                        idx += 3;
                                    }
                                    else if(temp[i].length() >= 2 && temp[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                        sb.append("||");
                                        idx += 2;
                                    }
                                    else{
                                        sb.append(temp[i].substring(idx, idx + 1));
                                        idx += 1;                                        
                                    }
                                }
                                else{
                                    sb.append(temp[i].substring(idx, idx + 1));
                                    idx += 1;                                    
                                }

                            }
                        }
                    }
                    if(sb.length() != 0){
                        if(conditions.length() != 0){
                            conditions.append(" " + sb.toString());
                            sb.delete(0, sb.length());
                        }
                        else{
                            conditions.append(sb.toString());
                            sb.delete(0, sb.length());
                        }
                    }
                }

                String conditions_postfix = makePostfix("( " + conditions.toString() + " )");

                System.out.printf("\n%s >> %s", "Database Name", database_name);
                System.out.println();
                System.out.printf("%s >> %s", "Table", tables);
                System.out.println();
                System.out.printf("%s >> %s", "Condition", conditions);
                System.out.println();
                System.out.printf("%s >> %s", "Prefix Condition", conditions_postfix);
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
