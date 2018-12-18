package dhsong;

import dhsong.ClientConnection;
import dhsong.Protocol.Command;
import dhsong.SafeEncoder;
import dhsong.SQL;
import dhsong.Key;

import java.util.Arrays;
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
        String DATABASE = "BaseDB";
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
                    System.out.printf("%s >> %s", "Attribute", parsed.get(0));
                    System.out.println();
                    System.out.printf("%s >> %s", "Table", parsed.get(1));
                    System.out.println();
                    System.out.printf("%s >> %s", "Condition", parsed.get(2));
                    System.out.println();
                    System.out.printf("%s >> %s", "Group", parsed.get(3));
                    System.out.println();
                }
                else{
                    parsed = parse.parseSelect(command);
                    //System.out.printf("\n%s >> %s", "Database Name", DATABASE);
                    //System.out.println();
                    //System.out.printf("%s >> %s", "Attribute", parsed.get(0));
                    //System.out.println();
                    //System.out.printf("%s >> %s", "Table", parsed.get(1));
                    //System.out.println();
                    System.out.printf("%s >> %s", "Condition", parsed.get(2));
                    //System.out.println();

                    String[] tables = parsed.get(1).split(",");

                    List<String> attributes = Arrays.asList(parsed.get(0).split(","));
                    for(int i = 0; i < attributes.size(); i++){
                        if(attributes.get(i).length() >= 4 && attributes.get(i).substring(0, 4).equals("sum(")){
                            attributes.set(i, attributes.get(i).substring(4, attributes.get(i).length() - 1));
                        }
                        else if(attributes.get(i).length() >= 6 && attributes.get(i).substring(0, 6).equals("count(")){
                            attributes.set(i, attributes.get(i).substring(6, attributes.get(i).length() - 1));
                        }
                        else if(attributes.get(i).length() >= 4 && attributes.get(i).substring(0, 4).equals("avg(")){
                            attributes.set(i, attributes.get(i).substring(4, attributes.get(i).length() - 1));
                        }
                    }

                    List<String> parameter = new ArrayList<>();
                    String buf;
                    
                    if(parsed.get(0).equals("*")){
                        client.sendCommand(Command.HKEYS, Key.getKeyTable(client, BASE, DATABASE, tables[0]));
                        ros_list = client.getMultiBulkReply();
                        attributes = ros_list;

                        for(int i = 0; i < attributes.size(); i++){
                            for(int j = 0; j < tables.length; j++){
                                buf = Key.getKeyAttribute(client, BASE, DATABASE, tables[j], attributes.get(i));
                                if(buf != null){
                                    attributes.set(i, buf);
                                    parameter.add(Key.getKeyTable(client, BASE, DATABASE, tables[j]) + " " + attributes.get(i));
                                }
                            }
                        }    
                    }
                    else{
                        for(int i = 0; i < attributes.size(); i++){
                            for(int j = 0; j < tables.length; j++){
                                if((attributes.get(i).indexOf(".") != -1) && (attributes.get(i).split(".")[0].equals(tables[j]))){
                                    buf = Key.getKeyAttribute(client, BASE, DATABASE, tables[j], attributes.get(i).split(".")[1]);
                                    if(buf != null){
                                        attributes.set(i, buf);
                                        parameter.add(Key.getKeyTable(client, BASE, DATABASE, tables[j]) + " " + attributes.get(i));
                                    }    
                                }
                                else{
                                    buf = Key.getKeyAttribute(client, BASE, DATABASE, tables[j], attributes.get(i));
                                    if(buf != null){
                                        attributes.set(i, buf);
                                        parameter.add(Key.getKeyTable(client, BASE, DATABASE, tables[j]) + " " + attributes.get(i));
                                    }
                                }
                            }
                        }    
                    }


                    StringBuilder sb_attribute = new StringBuilder();
                    for(int i = 0; i < parameter.size(); i++){
                        if(sb_attribute.length() == 0){
                            sb_attribute.append(parameter.get(i));
                        }
                        else{
                            sb_attribute.append(" " + parameter.get(i));
                        }
                    }


                    
                    //client.sendCommand(Command.SELECTFROM, SafeEncoder.encode(parsed.get(2)), SafeEncoder.encode(sb_attribute.toString()));


                    client.sendCommand(Command.SELECTFROM, SafeEncoder.encode(parsed.get(2)), SafeEncoder.encode(sb_attribute.toString()), SafeEncoder.encode("NONCONCRETE"));
                    ros_str = client.getStatusCodeReply();
                    //System.out.println(ros_str);
                    StringBuilder result_buf = new StringBuilder();
                    try{
                        while(true){
                            client.sendCommand(Command.GETLEFT);
                            ros_str = client.getBulkReply();
                            System.out.println(ros_str);
                            result_buf.append(ros_str);
                        }
                    }
                    catch(Exception e){
                    }

                    List<String> result_record = new ArrayList<>();
                    List<StringBuilder> result_attribute = new ArrayList<>();
                    List<String> result_line = new ArrayList<>();

                    
                    result_line = Arrays.asList(result_buf.toString().split("\n"));
                    for(int i = 0; i < result_line.size(); i++){
                        if(result_line.get(i).length() == 0){
                            result_line.remove(i);
                        }
                    }

                    for(int i = 0; i < result_line.size() - 1 ; i++){
                        result_line.set(i, result_line.get(i).substring(0, result_line.get(i).length() - 1));
                        result_record = Arrays.asList(result_line.get(i).split(","));
                        if(i == 0){
                            for(int j = 0; j < result_record.size(); j++){
                                result_attribute.add(new StringBuilder(result_record.get(j).split("[.]")[1] + ":"));
                            }
                        }
                        else{
                            for(int j = 0; j < result_record.size(); j++){
                                StringBuilder temp = result_attribute.get(j);
                                temp.append(result_record.get(j));
                                result_attribute.set(j, temp.append(","));
                            }
                        }
                    }

                    //System.out.println(result_attribute);
                    //System.out.println(result_attribute.size());


                    attributes = Arrays.asList(parsed.get(0).split(","));
                    String[] value;
                    int sum = 0;
                    int count = 0;
                    double avg = 0.0;
                    for(int i = 0; i < attributes.size(); i++){
                        if(attributes.get(i).length() >= 4 && attributes.get(i).substring(0, 4).equals("sum(")){
                            sum = 0;
                            for(int j = 0; j < result_attribute.size(); j++){
                                if(result_attribute.get(j).toString().split(":")[0].indexOf(attributes.get(i).substring(4, attributes.get(i).length() - 1)) == 0){
                                    value = result_attribute.get(j).toString().substring(0, result_attribute.get(i).length() - 1).split(":")[1].split(",");
                                    for(int k = 0; k < value.length; k++){
                                        sum += Integer.parseInt(value[k]);
                                    }
                                    System.out.println("Sum of " + attributes.get(i).substring(4, attributes.get(i).length() - 1) + " = " + sum);
                                }
                            }
                        }
                        else if(attributes.get(i).length() >= 6 && attributes.get(i).substring(0, 6).equals("count(")){
                            count = 0;
                            for(int j = 0; j < result_attribute.size(); j++){
                                if(result_attribute.get(j).toString().split(":")[0].indexOf(attributes.get(i).substring(6, attributes.get(i).length() - 1)) == 0){
                                    value = result_attribute.get(j).toString().substring(0, result_attribute.get(i).length() - 1).split(":")[1].split(",");
                                    count = value.length;
                                    System.out.println("Count of " + attributes.get(i).substring(6, attributes.get(i).length() - 1) + " = " + count);
                                }
                            }
                        }
                        else if(attributes.get(i).length() >= 4 && attributes.get(i).substring(0, 4).equals("avg(")){
                            sum = 0;
                            count = 0;
                            for(int j = 0; j < result_attribute.size(); j++){
                                if(result_attribute.get(j).toString().split(":")[0].indexOf(attributes.get(i).substring(4, attributes.get(i).length() - 1)) == 0){
                                    value = result_attribute.get(j).toString().substring(0, result_attribute.get(i).length() - 1).split(":")[1].split(",");
                                    for(int k = 0; k < value.length; k++){
                                        sum += Integer.parseInt(value[k]);
                                    }
                                    count = value.length;
                                    avg = (double)sum / (double)count;
                                    System.out.println("Average of " + attributes.get(i).substring(4, attributes.get(i).length() - 1) + " = " + sum);    
                                }
                            }
                        }
                    }

                }

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
