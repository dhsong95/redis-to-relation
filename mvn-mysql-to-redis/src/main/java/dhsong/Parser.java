package dhsong;

import java.util.ArrayList;
import java.util.Stack;

class Parser {
    private String str;
    private ArrayList<String> str_parsed;

    public Parser(){
        this.str = new String();
        this.str_parsed = new ArrayList<String>();
    }
    public Parser(String str){
        this.str_parsed = new ArrayList<String>();
        this.str = str;
    }
    public String getStr(){
        return str;
    }
    public void setStr(String str){
        this.str = str;
    }
    public ArrayList<String> getStrParsed(){
        return str_parsed;
    } 

    public ArrayList<String> parseCreate(String str){
        this.str_parsed.clear();
        this.setStr(str);
        String[] table_attribute = this.str.split("[(]");
        String[] command_table = table_attribute[0].split(" ");
        String table = command_table[command_table.length - 1];

        this.str_parsed.add(table);

        String[] attribute_attribute = table_attribute[1].split(",");
        StringBuilder attributes = new StringBuilder();
        StringBuilder types = new StringBuilder();

        for(int i=0; i<attribute_attribute.length; i++){
            
            if(attribute_attribute[i].charAt(0) == ' '){
                attribute_attribute[i] = attribute_attribute[i].substring(1, attribute_attribute[i].length());
            }
            
            if(attribute_attribute[i].charAt(attribute_attribute[i].length()-1) == ';'){
                attribute_attribute[i] = attribute_attribute[i].substring(0, attribute_attribute[i].length() - 2);
            }

            String[] attribute_type = attribute_attribute[i].split(" ");
     
            if(i == 0){
                attributes.append(attribute_type[0]);
                types.append(attribute_type[1].toUpperCase());
            }
            else{
                attributes.append(","+attribute_type[0]);
                types.append(","+attribute_type[1].toUpperCase());
            }
        }
        this.str_parsed.add(attributes.toString());
        this.str_parsed.add(types.toString());

        return this.getStrParsed();
    }

    public ArrayList<String> parseInsert(String str){
        this.str_parsed.clear();
        this.setStr(str);

        String[] table_value = str.split("[(]");
        String[] command_table = table_value[0].split(" ");
        String table = command_table[command_table.length - 2];

        this.str_parsed.add(table);

        String[] value_value = table_value[1].split(",");
        StringBuilder values = new StringBuilder();

        for(int i=0; i<value_value.length; i++){
            if(value_value[i].charAt(0) == ' '){
                value_value[i] = value_value[i].substring(1, value_value[i].length());
            }
            if(value_value[i].charAt(value_value[i].length()-1) == ';'){
                value_value[i] = value_value[i].substring(0, value_value[i].length() - 2);
            }

            if(i == 0){
                values.append(value_value[i]);
            }
            else{
                values.append("," + value_value[i]);
            }

        }

        this.str_parsed.add(values.toString());

        return this.getStrParsed();
    }

    public ArrayList<String> parseSelect(String str){
        this.str_parsed.clear();
        this.setStr(str);

        String[] select_from_where = str.split(" ");
        int idx_select = -1;
        int idx_from = -1;
        int idx_where = -1;

        for(int i = 0; i < select_from_where.length; i++){
            if(select_from_where[i].toLowerCase().equals("select")){
                idx_select = i;
            }
            else if(select_from_where[i].toLowerCase().equals("from")){
                idx_from = i;
            }
            else if(select_from_where[i].toLowerCase().equals("where")){
                idx_where = i;
            }
        }
        if(idx_where == -1){
            idx_where = select_from_where.length;
        }

        StringBuilder attributes = new StringBuilder();
        for(int i = idx_select + 1; i < idx_from; i++){
            if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ','){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
                                
            if(i == idx_select + 1){
                attributes.append(select_from_where[i]);
            }
            else{
                attributes.append(","+select_from_where[i]);                        
            }
        }

        this.str_parsed.add(attributes.toString());

        StringBuilder tables = new StringBuilder();
        for(int i = idx_from + 1; i < idx_where; i++){
            if(select_from_where[i].charAt(select_from_where[i].length()-1) == ','){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
            if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ';'){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
            
            if(i == idx_from + 1){
                tables.append(select_from_where[i]);
            }
            else{
                tables.append(","+select_from_where[i]);                        
            }
        }

        this.str_parsed.add(tables.toString());

        StringBuilder conditions = new StringBuilder();
        for(int i = idx_where + 1; i < select_from_where.length; i++){
            if(select_from_where[i].charAt(select_from_where[i].length()-1) == ';'){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
            

            int idx = 0;
            StringBuilder buf = new StringBuilder();

            while(idx < select_from_where[i].length()){

                if(select_from_where[i].length() == 1){
                    if(conditions.length() == 0){
                        conditions.append(select_from_where[i]);
                    }
                    else{
                        conditions.append(" " + select_from_where[i]);
                    }
                    idx += 1;
                }
                else{
                    if(select_from_where[i].substring(idx, idx + 1).equals("(")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("(");
                            }
                            else{
                                conditions.append(buf + " (");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" (");
                            }
                            else{
                                conditions.append(" " + buf + " (");
                            }
                        }
                        idx += 1;                            
                        buf.delete(0, buf.length());
                    }
                    else if(select_from_where[i].substring(idx, idx + 1).equals(")")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append(")");
                            }
                            else{
                                conditions.append(buf + " )");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" )");
                            }
                            else{
                                conditions.append(" " + buf + " )");
                            }
                        }
                        buf.delete(0, buf.length());
                        idx += 1;         
                    }
                    else if(select_from_where[i].substring(idx, idx + 1).equals("=")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("=");
                            }
                            else{
                                conditions.append(buf + " =");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" =");
                            }
                            else{
                                conditions.append(" " + buf + " =");
                            }
                        }
                        idx += 1;
                        buf.delete(0, buf.length());                                                       
                    }
                    else if(select_from_where[i].substring(idx, idx + 1).equals("<")){
                        if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("<=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<=");
                                }
                                else{
                                    conditions.append(buf + " <=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <=");
                                }
                                else{
                                    conditions.append(" " + buf + " <=");
                                }
                            }
                            idx += 2;
                            buf.delete(0, buf.length());                                                                                               
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<");
                                }
                                else{
                                    conditions.append(buf + " <");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <");
                                }
                                else{
                                    conditions.append(" " + buf + " <");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(select_from_where[i].substring(idx, idx + 1).equals(">")){
                        if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals(">=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">=");
                                }
                                else{
                                    conditions.append(buf + " >=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >=");
                                }
                                else{
                                    conditions.append(" " + buf + " >=");
                                }
                            }
                            idx += 2;                           
                            buf.delete(0, buf.length());    
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">");
                                }
                                else{
                                    conditions.append(buf + " >");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >");
                                }
                                else{
                                    conditions.append(" " + buf + " >");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("&&")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("&&");
                            }
                            else{
                                conditions.append(buf + " &&");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" &&");
                            }
                            else{
                                conditions.append(" " + buf + " &&");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("||")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("||");
                            }
                            else{
                                conditions.append(buf + " ||");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" ||");
                            }
                            else{
                                conditions.append(" " + buf + " ||");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else{
                        if(idx == 0){
                            if(select_from_where[i].length() >= 3 && select_from_where[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                buf.append("&&");
                                idx += 3;
                            }
                            else if(select_from_where[i].length() >= 2 && select_from_where[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                buf.append("||");
                                idx += 2;
                            }
                            else{
                                buf.append(select_from_where[i].substring(idx, idx + 1));
                                idx += 1;                                        
                            }
                        }
                        else{
                            buf.append(select_from_where[i].substring(idx, idx + 1));
                            idx += 1;                                    
                        }
                    }
                }
            }

            if(buf.length() != 0){
                if(conditions.length() != 0){
                    conditions.append(" " + buf);
                    buf.delete(0, buf.length());
                }
                else{
                    conditions.append(buf);
                    buf.delete(0, buf.length());
                }
            }
        }

        String[] conditions_logic = conditions.toString().split(" ");
        StringBuilder conditions_logic_paren = new StringBuilder();
        int idx = 0;
        int logic_start = 0;
        int logic_end = 0;
        while(idx < conditions_logic.length){
            if(conditions_logic[idx].equals("&&") || conditions_logic[idx].equals("||")){
                logic_end = idx;
                conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
                conditions_logic[logic_end] = ") " + conditions_logic[logic_end];
                logic_start = logic_end + 1;
            }
            idx += 1;
        }

        conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
        conditions_logic[conditions_logic.length - 1] = conditions_logic[conditions_logic.length - 1] + " )";

        for(int i = 0; i < conditions_logic.length; i++){
            if(i == conditions_logic.length - 1){
                conditions_logic_paren.append(conditions_logic[i]);
            }
            else{
                conditions_logic_paren.append(conditions_logic[i] + " ");
            }
        }
        System.out.println(conditions_logic_paren);
        String conditions_postfix = makePostfix("( " + conditions_logic_paren.toString() + " )");
        
        this.str_parsed.add(conditions_postfix);
        return this.getStrParsed();
    }

    public ArrayList<String> parseGroup(String str){
        this.str_parsed.clear();
        this.setStr(str);

        String[] select_from_where = str.split(" ");
        int idx_select = -1;
        int idx_from = -1;
        int idx_where = -1;
        int idx_group = -1;

        for(int i = 0; i < select_from_where.length; i++){
            if(select_from_where[i].toLowerCase().equals("select")){
                idx_select = i;
            }
            else if(select_from_where[i].toLowerCase().equals("from")){
                idx_from = i;
            }
            else if(select_from_where[i].toLowerCase().equals("where")){
                idx_where = i;
            }
            else if(i <= select_from_where.length - 2 && select_from_where[i].toLowerCase().equals("group") && select_from_where[i + 1].toLowerCase().equals("by")){
                idx_group = i;
            }
        }

        StringBuilder attributes = new StringBuilder();
        for(int i = idx_select + 1; i < idx_from; i++){
            if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ','){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
                                
            if(i == idx_select + 1){
                attributes.append(select_from_where[i]);
            }
            else{
                attributes.append(","+select_from_where[i]);                        
            }
        }

        this.str_parsed.add(attributes.toString());

        StringBuilder tables = new StringBuilder();
        if(idx_where == -1){
            for(int i = idx_from + 1; i < idx_group; i++){
                if(select_from_where[i].charAt(select_from_where[i].length()-1) == ','){
                    select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
                }
                if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ';'){
                    select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
                }
                
                if(i == idx_from + 1){
                    tables.append(select_from_where[i]);
                }
                else{
                    tables.append(","+select_from_where[i]);                        
                }
            }    

        }
        else{
            for(int i = idx_from + 1; i < idx_where; i++){
                if(select_from_where[i].charAt(select_from_where[i].length()-1) == ','){
                    select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
                }
                if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ';'){
                    select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
                }
                
                if(i == idx_from + 1){
                    tables.append(select_from_where[i]);
                }
                else{
                    tables.append(","+select_from_where[i]);                        
                }
            }    
        }

        this.str_parsed.add(tables.toString());

        if(idx_where != -1){
            StringBuilder conditions = new StringBuilder();
            for(int i = idx_where + 1; i < idx_group; i++){
                if(select_from_where[i].charAt(select_from_where[i].length()-1) == ';'){
                    select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
                }
                
    
                int idx = 0;
                StringBuilder buf = new StringBuilder();
    
                while(idx < select_from_where[i].length()){
    
                    if(select_from_where[i].length() == 1){
                        if(conditions.length() == 0){
                            conditions.append(select_from_where[i]);
                        }
                        else{
                            conditions.append(" " + select_from_where[i]);
                        }
                        idx += 1;
                    }
                    else{
                        if(select_from_where[i].substring(idx, idx + 1).equals("(")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("(");
                                }
                                else{
                                    conditions.append(buf + " (");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" (");
                                }
                                else{
                                    conditions.append(" " + buf + " (");
                                }
                            }
                            idx += 1;                            
                            buf.delete(0, buf.length());
                        }
                        else if(select_from_where[i].substring(idx, idx + 1).equals(")")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(")");
                                }
                                else{
                                    conditions.append(buf + " )");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" )");
                                }
                                else{
                                    conditions.append(" " + buf + " )");
                                }
                            }
                            buf.delete(0, buf.length());
                            idx += 1;         
                        }
                        else if(select_from_where[i].substring(idx, idx + 1).equals("=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("=");
                                }
                                else{
                                    conditions.append(buf + " =");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" =");
                                }
                                else{
                                    conditions.append(" " + buf + " =");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                       
                        }
                        else if(select_from_where[i].substring(idx, idx + 1).equals("<")){
                            if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("<=")){
                                if(conditions.length() == 0){
                                    if(buf.length() == 0){
                                        conditions.append("<=");
                                    }
                                    else{
                                        conditions.append(buf + " <=");
                                    }
                                }
                                else{
                                    if(buf.length() == 0){
                                        conditions.append(" <=");
                                    }
                                    else{
                                        conditions.append(" " + buf + " <=");
                                    }
                                }
                                idx += 2;
                                buf.delete(0, buf.length());                                                                                               
                            }
                            else{
                                if(conditions.length() == 0){
                                    if(buf.length() == 0){
                                        conditions.append("<");
                                    }
                                    else{
                                        conditions.append(buf + " <");
                                    }
                                }
                                else{
                                    if(buf.length() == 0){
                                        conditions.append(" <");
                                    }
                                    else{
                                        conditions.append(" " + buf + " <");
                                    }
                                }
                                idx += 1;
                                buf.delete(0, buf.length());                                                           
                            }
                        }
                        else if(select_from_where[i].substring(idx, idx + 1).equals(">")){
                            if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals(">=")){
                                if(conditions.length() == 0){
                                    if(buf.length() == 0){
                                        conditions.append(">=");
                                    }
                                    else{
                                        conditions.append(buf + " >=");
                                    }
                                }
                                else{
                                    if(buf.length() == 0){
                                        conditions.append(" >=");
                                    }
                                    else{
                                        conditions.append(" " + buf + " >=");
                                    }
                                }
                                idx += 2;                           
                                buf.delete(0, buf.length());    
                            }
                            else{
                                if(conditions.length() == 0){
                                    if(buf.length() == 0){
                                        conditions.append(">");
                                    }
                                    else{
                                        conditions.append(buf + " >");
                                    }
                                }
                                else{
                                    if(buf.length() == 0){
                                        conditions.append(" >");
                                    }
                                    else{
                                        conditions.append(" " + buf + " >");
                                    }
                                }
                                idx += 1;
                                buf.delete(0, buf.length());                                                           
                            }
                        }
                        else if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("&&")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("&&");
                                }
                                else{
                                    conditions.append(buf + " &&");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" &&");
                                }
                                else{
                                    conditions.append(" " + buf + " &&");
                                }
                            }
                            idx += 2;                           
                            buf.delete(0, buf.length());    
                        }
                        else if(idx <= select_from_where[i].length() - 2 && select_from_where[i].substring(idx, idx + 2).equals("||")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("||");
                                }
                                else{
                                    conditions.append(buf + " ||");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" ||");
                                }
                                else{
                                    conditions.append(" " + buf + " ||");
                                }
                            }
                            idx += 2;                           
                            buf.delete(0, buf.length());    
                        }
                        else{
                            if(idx == 0){
                                if(select_from_where[i].length() >= 3 && select_from_where[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                    buf.append("&&");
                                    idx += 3;
                                }
                                else if(select_from_where[i].length() >= 2 && select_from_where[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                    buf.append("||");
                                    idx += 2;
                                }
                                else{
                                    buf.append(select_from_where[i].substring(idx, idx + 1));
                                    idx += 1;                                        
                                }
                            }
                            else{
                                buf.append(select_from_where[i].substring(idx, idx + 1));
                                idx += 1;                                    
                            }
                        }
                    }
                }
    
                if(buf.length() != 0){
                    if(conditions.length() != 0){
                        conditions.append(" " + buf);
                        buf.delete(0, buf.length());
                    }
                    else{
                        conditions.append(buf);
                        buf.delete(0, buf.length());
                    }
                }
            }
    
            String conditions_postfix = makePostfix("( " + conditions.toString() + " )");
    
            this.str_parsed.add(conditions_postfix);    
        }
        else{
            this.str_parsed.add("");
        }


        StringBuilder groups = new StringBuilder();
        for(int i = idx_group + 2; i < select_from_where.length; i++){
            if(select_from_where[i].charAt(select_from_where[i].length()-1) == ','){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
            if(select_from_where[i].charAt(select_from_where[i].length() - 1) == ';'){
                select_from_where[i] = select_from_where[i].substring(0, select_from_where[i].length() - 1);
            }
            
            if(i == idx_group + 2){
                groups.append(select_from_where[i]);
            }
            else{
                groups.append(","+select_from_where[i]);                        
            }
        }
        this.str_parsed.add(groups.toString());
        
        return this.getStrParsed();
    }


    public ArrayList<String> parseUpdate(String str){
        this.str_parsed.clear();
        this.setStr(str);

        String[] update_set_where = this.str.split(" ");
        int idx_update = -1;
        int idx_set = -1;
        int idx_where = -1;

        for(int i = 0; i < update_set_where.length; i++){
            if(update_set_where[i].toLowerCase().equals("update")){
                idx_update = i;
            }
            else if(update_set_where[i].toLowerCase().equals("set")){
                idx_set = i;
            }
            else if(update_set_where[i].toLowerCase().equals("where")){
                idx_where = i;
            }
        }
        if(idx_where == -1){
            idx_where = update_set_where.length;
        }


        StringBuilder tables = new StringBuilder();
        for(int i = idx_update + 1; i < idx_set; i++){
            if(update_set_where[i].charAt(update_set_where[i].length() - 1) == ','){
                update_set_where[i] = update_set_where[i].substring(0, update_set_where[i].length() - 1);
            }
            
            if(i == idx_update + 1){
                tables.append(update_set_where[i]);
            }
            else{
                tables.append(","+update_set_where[i]);                        
            }
        }
        this.str_parsed.add(tables.toString());

        StringBuilder updates = new StringBuilder();
        for(int i = idx_set + 1; i < idx_where; i++){
            if(update_set_where[i].charAt(update_set_where[i].length()-1) == ','){
                update_set_where[i] = update_set_where[i].substring(0, update_set_where[i].length() - 1);
            }

            int idx = 0;
            StringBuilder buf = new StringBuilder();

            while(idx < update_set_where[i].length()){

                if(update_set_where[i].length() == 1){
                    if(updates.length() == 0){
                        updates.append(update_set_where[i]);
                    }
                    else{
                        updates.append(" " + update_set_where[i]);
                    }
                    idx += 1;
                }
                else{
                    if(update_set_where[i].substring(idx, idx + 1).equals("(")){
                        if(updates.length() == 0){
                            if(buf.length() == 0){
                                updates.append("(");
                            }
                            else{
                                updates.append(buf + " (");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                updates.append(" (");
                            }
                            else{
                                updates.append(" " + buf + " (");
                            }
                        }
                        idx += 1;                            
                        buf.delete(0, buf.length());
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals(")")){
                        if(updates.length() == 0){
                            if(buf.length() == 0){
                                updates.append(")");
                            }
                            else{
                                updates.append(buf + " )");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                updates.append(" )");
                            }
                            else{
                                updates.append(" " + buf + " )");
                            }
                        }
                        buf.delete(0, buf.length());
                        idx += 1;         
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals("=")){
                        if(updates.length() == 0){
                            if(buf.length() == 0){
                                updates.append("=");
                            }
                            else{
                                updates.append(buf + " =");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                updates.append(" =");
                            }
                            else{
                                updates.append(" " + buf + " =");
                            }
                        }
                        idx += 1;
                        buf.delete(0, buf.length());                                                       
                    }
                    else{
                        buf.append(update_set_where[i].substring(idx, idx + 1));
                        idx += 1;                                    
                    }
                }
            }
            if(buf.length() != 0){
                if(updates.length() != 0){
                    updates.append(" " + buf);
                    buf.delete(0, buf.length());
                }
                else{
                    updates.append(buf);
                    buf.delete(0, buf.length());
                }
            }            
        }


        String updates_postfix = makePostfix("( " + updates.toString() + " )");
        this.str_parsed.add(updates_postfix);

        StringBuilder conditions = new StringBuilder();
        for(int i = idx_where + 1; i < update_set_where.length; i++){
            if(update_set_where[i].charAt(update_set_where[i].length()-1) == ';'){
                update_set_where[i] = update_set_where[i].substring(0, update_set_where[i].length() - 1);
            }
            

            int idx = 0;
            StringBuilder buf = new StringBuilder();

            while(idx < update_set_where[i].length()){

                if(update_set_where[i].length() == 1){
                    if(conditions.length() == 0){
                        conditions.append(update_set_where[i]);
                    }
                    else{
                        conditions.append(" " + update_set_where[i]);
                    }
                    idx += 1;
                }
                else{
                    if(update_set_where[i].substring(idx, idx + 1).equals("(")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("(");
                            }
                            else{
                                conditions.append(buf + " (");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" (");
                            }
                            else{
                                conditions.append(" " + buf + " (");
                            }
                        }
                        idx += 1;                            
                        buf.delete(0, buf.length());
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals(")")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append(")");
                            }
                            else{
                                conditions.append(buf + " )");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" )");
                            }
                            else{
                                conditions.append(" " + buf + " )");
                            }
                        }
                        buf.delete(0, buf.length());
                        idx += 1;         
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals("=")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("=");
                            }
                            else{
                                conditions.append(buf + " =");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" =");
                            }
                            else{
                                conditions.append(" " + buf + " =");
                            }
                        }
                        idx += 1;
                        buf.delete(0, buf.length());                                                       
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals("<")){
                        if(idx <= update_set_where[i].length() - 2 && update_set_where[i].substring(idx, idx + 2).equals("<=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<=");
                                }
                                else{
                                    conditions.append(buf + " <=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <=");
                                }
                                else{
                                    conditions.append(" " + buf + " <=");
                                }
                            }
                            idx += 2;
                            buf.delete(0, buf.length());                                                                                               
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<");
                                }
                                else{
                                    conditions.append(buf + " <");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <");
                                }
                                else{
                                    conditions.append(" " + buf + " <");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(update_set_where[i].substring(idx, idx + 1).equals(">")){
                        if(idx <= update_set_where[i].length() - 2 && update_set_where[i].substring(idx, idx + 2).equals(">=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">=");
                                }
                                else{
                                    conditions.append(buf + " >=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >=");
                                }
                                else{
                                    conditions.append(" " + buf + " >=");
                                }
                            }
                            idx += 2;                           
                            buf.delete(0, buf.length());    
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">");
                                }
                                else{
                                    conditions.append(buf + " >");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >");
                                }
                                else{
                                    conditions.append(" " + buf + " >");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(idx <= update_set_where[i].length() - 2 && update_set_where[i].substring(idx, idx + 2).equals("&&")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("&&");
                            }
                            else{
                                conditions.append(buf + " &&");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" &&");
                            }
                            else{
                                conditions.append(" " + buf + " &&");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else if(idx <= update_set_where[i].length() - 2 && update_set_where[i].substring(idx, idx + 2).equals("||")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("||");
                            }
                            else{
                                conditions.append(buf + " ||");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" ||");
                            }
                            else{
                                conditions.append(" " + buf + " ||");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else{
                        if(idx == 0){
                            if(update_set_where[i].length() >= 3 && update_set_where[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                buf.append("&&");
                                idx += 3;
                            }
                            else if(update_set_where[i].length() >= 2 && update_set_where[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                buf.append("||");
                                idx += 2;
                            }
                            else{
                                buf.append(update_set_where[i].substring(idx, idx + 1));
                                idx += 1;                                        
                            }
                        }
                        else{
                            buf.append(update_set_where[i].substring(idx, idx + 1));
                            idx += 1;                                    
                        }

                    }
                }
            }
            if(buf.length() != 0){
                if(conditions.length() != 0){
                    conditions.append(" " + buf.toString());
                    buf.delete(0, buf.length());
                }
                else{
                    conditions.append(buf.toString());
                    buf.delete(0, buf.length());
                }
            }
        }

        String[] conditions_logic = conditions.toString().split(" ");
        StringBuilder conditions_logic_paren = new StringBuilder();
        int idx = 0;
        int logic_start = 0;
        int logic_end = 0;
        while(idx < conditions_logic.length){
            if(conditions_logic[idx].equals("&&") || conditions_logic[idx].equals("||")){
                logic_end = idx;
                conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
                conditions_logic[logic_end] = ") " + conditions_logic[logic_end];
                logic_start = logic_end + 1;
            }
            idx += 1;
        }

        conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
        conditions_logic[conditions_logic.length - 1] = conditions_logic[conditions_logic.length - 1] + " )";


        for(int i = 0; i < conditions_logic.length; i++){
            if(i == conditions_logic.length - 1){
                conditions_logic_paren.append(conditions_logic[i]);
            }
            else{
                conditions_logic_paren.append(conditions_logic[i] + " ");
            }
        }
        System.out.println(conditions_logic_paren);
        String conditions_postfix = makePostfix("( " + conditions_logic_paren.toString() + " )");
        
        this.str_parsed.add(conditions_postfix);
        return this.getStrParsed();
    }

    public ArrayList<String> parseDelete(String str){
        this.str_parsed.clear();
        this.setStr(str);
        String[] delete_where = this.str.split(" ");
        int idx_from = -1;
        int idx_where = -1;

        for(int i = 0; i < delete_where.length; i++){
            if(delete_where[i].toLowerCase().equals("from")){
                idx_from = i;
            }
            else if(delete_where[i].toLowerCase().equals("where")){
                idx_where = i;
            }
        }
        if(idx_where == -1){
            idx_where = delete_where.length;
        }

        StringBuilder tables = new StringBuilder();
        for(int i = idx_from + 1; i < idx_where; i++){
            if(delete_where[i].charAt(delete_where[i].length()-1) == ','){
                delete_where[i] = delete_where[i].substring(0, delete_where[i].length() - 1);
            }
            
            if(i == idx_from + 1){
                tables.append(delete_where[i]);
            }
            else{
                tables.append(","+delete_where[i]);                        
            }
        }

        this.str_parsed.add(tables.toString());

        StringBuilder conditions = new StringBuilder();
        for(int i = idx_where + 1; i < delete_where.length; i++){
            if(delete_where[i].charAt(delete_where[i].length()-1) == ';'){
                delete_where[i] = delete_where[i].substring(0, delete_where[i].length() - 1);
            }
            

            int idx = 0;
            StringBuilder buf = new StringBuilder();

            while(idx < delete_where[i].length()){

                if(delete_where[i].length() == 1){
                    if(conditions.length() == 0){
                        conditions.append(delete_where[i]);
                    }
                    else{
                        conditions.append(" " + delete_where[i]);
                    }
                    idx += 1;
                }
                else{
                    if(delete_where[i].substring(idx, idx + 1).equals("(")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("(");
                            }
                            else{
                                conditions.append(buf + " (");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" (");
                            }
                            else{
                                conditions.append(" " + buf + " (");
                            }
                        }
                        idx += 1;                            
                        buf.delete(0, buf.length());
                    }
                    else if(delete_where[i].substring(idx, idx + 1).equals(")")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append(")");
                            }
                            else{
                                conditions.append(buf + " )");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" )");
                            }
                            else{
                                conditions.append(" " + buf + " )");
                            }
                        }
                        buf.delete(0, buf.length());
                        idx += 1;         
                    }
                    else if(delete_where[i].substring(idx, idx + 1).equals("=")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("=");
                            }
                            else{
                                conditions.append(buf + " =");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" =");
                            }
                            else{
                                conditions.append(" " + buf + " =");
                            }
                        }
                        idx += 1;
                        buf.delete(0, buf.length());                                                       
                    }
                    else if(delete_where[i].substring(idx, idx + 1).equals("<")){
                        if(idx <= delete_where[i].length() - 2 && delete_where[i].substring(idx, idx + 2).equals("<=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<=");
                                }
                                else{
                                    conditions.append(buf + " <=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <=");
                                }
                                else{
                                    conditions.append(" " + buf + " <=");
                                }
                            }
                            idx += 2;
                            buf.delete(0, buf.length());                                                                                               
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append("<");
                                }
                                else{
                                    conditions.append(buf + " <");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" <");
                                }
                                else{
                                    conditions.append(" " + buf + " <");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(delete_where[i].substring(idx, idx + 1).equals(">")){
                        if(idx <= delete_where[i].length() - 2 && delete_where[i].substring(idx, idx + 2).equals(">=")){
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">=");
                                }
                                else{
                                    conditions.append(buf + " >=");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >=");
                                }
                                else{
                                    conditions.append(" " + buf + " >=");
                                }
                            }
                            idx += 2;                           
                            buf.delete(0, buf.length());    
                        }
                        else{
                            if(conditions.length() == 0){
                                if(buf.length() == 0){
                                    conditions.append(">");
                                }
                                else{
                                    conditions.append(buf + " >");
                                }
                            }
                            else{
                                if(buf.length() == 0){
                                    conditions.append(" >");
                                }
                                else{
                                    conditions.append(" " + buf + " >");
                                }
                            }
                            idx += 1;
                            buf.delete(0, buf.length());                                                           
                        }
                    }
                    else if(idx <= delete_where[i].length() - 2 && delete_where[i].substring(idx, idx + 2).equals("&&")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("&&");
                            }
                            else{
                                conditions.append(buf + " &&");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" &&");
                            }
                            else{
                                conditions.append(" " + buf + " &&");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else if(idx <= delete_where[i].length() - 2 && delete_where[i].substring(idx, idx + 2).equals("||")){
                        if(conditions.length() == 0){
                            if(buf.length() == 0){
                                conditions.append("||");
                            }
                            else{
                                conditions.append(buf + " ||");
                            }
                        }
                        else{
                            if(buf.length() == 0){
                                conditions.append(" ||");
                            }
                            else{
                                conditions.append(" " + buf + " ||");
                            }
                        }
                        idx += 2;                           
                        buf.delete(0, buf.length());    
                    }
                    else{
                        if(idx == 0){
                            if(delete_where[i].length() >= 3 && delete_where[i].substring(idx, idx + 3).toLowerCase().equals("and")){
                                buf.append("&&");
                                idx += 3;
                            }
                            else if(delete_where[i].length() >= 2 && delete_where[i].substring(idx, idx + 2).toLowerCase().equals("or")){
                                buf.append("||");
                                idx += 2;
                            }
                            else{
                                buf.append(delete_where[i].substring(idx, idx + 1));
                                idx += 1;                                        
                            }
                        }
                        else{
                            buf.append(delete_where[i].substring(idx, idx + 1));
                            idx += 1;                                    
                        }

                    }
                }
            }
            if(buf.length() != 0){
                if(conditions.length() != 0){
                    conditions.append(" " + buf);
                    buf.delete(0, buf.length());
                }
                else{
                    conditions.append(buf);
                    buf.delete(0, buf.length());
                }
            }
        }

        String[] conditions_logic = conditions.toString().split(" ");
        StringBuilder conditions_logic_paren = new StringBuilder();
        int idx = 0;
        int logic_start = 0;
        int logic_end = 0;
        while(idx < conditions_logic.length){
            if(conditions_logic[idx].equals("&&") || conditions_logic[idx].equals("||")){
                logic_end = idx;
                conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
                conditions_logic[logic_end] = ") " + conditions_logic[logic_end];
                logic_start = logic_end + 1;
            }
            idx += 1;
        }
        conditions_logic[logic_start] = "( " + conditions_logic[logic_start];
        conditions_logic[conditions_logic.length - 1] = conditions_logic[conditions_logic.length - 1] + " )";


        for(int i = 0; i < conditions_logic.length; i++){
            if(i == conditions_logic.length - 1){
                conditions_logic_paren.append(conditions_logic[i]);
            }
            else{
                conditions_logic_paren.append(conditions_logic[i] + " ");
            }
        }
        System.out.println(conditions_logic_paren);
        String conditions_postfix = makePostfix("( " + conditions_logic_paren.toString() + " )");
        
        this.str_parsed.add(conditions_postfix);
        return this.getStrParsed();
    }
    public String makePostfix(String op){
        
        StringBuilder result = new StringBuilder();
        String[] operations = op.split(" ");
        Stack<String> stack = new Stack<>();
        String top = new String();

        int idx = 0;
        while(idx < operations.length){
            if(operations[idx].equals("(")){
                stack.push(operations[idx]);
                idx += 1;
            }           
            else if(operations[idx].equals(")")){
                while(true){
                    top = stack.peek();
                    if(!stack.isEmpty() && top.equals("(")){
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
                top = stack.peek();
                if(top.equals("<=") || top.equals(">=") || top.equals("<") || top.equals(">") || top.equals("(") || top.equals("=")){
                    stack.push(operations[idx]);
                }
                else if(top.equals("&&") || top.equals("||")){
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
                top = stack.peek();
                
                while(stack.isEmpty() && !(top.equals("&&") || top.equals("||"))){
                    if(result.length() == 0){
                        result.append(stack.pop());
                    }
                    else{
                        result.append(" " + stack.pop());
                    }
                    top = stack.peek();
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

}