import javax.print.DocFlavor;
import java.sql.*;
import java.util.*;

//Projektaufgabe 1

public class Sofia {
    private static String url = "jdbc:postgresql://localhost:5432/DJRProjektaufgabe1";
    private static String user = "postgres";
    private static String pwd = "dreizehn13";
    private static Connection con;
    private static final Random RANDOM = new Random();
    static List<String> stringValues = Arrays.asList("a", "b", "c", "d", "e", "f");                  //list of possible String values
    static List<Integer> intValues = Arrays.asList(1, 2, 3, 4, 5, 6);                            //list of possible int values
    static Map<String, Integer> stringValueCounters = new HashMap<>();                     //list of counters for each possible String value
    static Map<Integer, Integer> intValueCounters = new HashMap<>();                       //list of counters for each possible int value
    static Map<String, Map<String, Integer>> stringAttributeRegister = new HashMap<>();    //register for each String attribute
    static Map<String, Map<Integer, Integer>> intAttributeRegister = new HashMap<>();      //register for each int attribute




    public static void main(String[] args) throws Exception {
        try {
            con = DriverManager.getConnection(url, user, pwd);
            System.out.println("Successfully Connected.");
        } catch (SQLException e) {
            System.out.println("Failed to Connect: " + e.getMessage());
        }

       showConnectionAndSQL();

        //generate(30, 0.2, 7);
        //h2v("h");
        //v2h("v");
    }

    // TODO: implement V2H
    // nicht Select oid, a1 from V where key = a2 and val b
    // sondern: Select oid, a1 from H where a2 = b
    // insert into v2h (Ai) Values (attWert);



    public static void v2h(String tableName) throws SQLException {
        //Delete if already exists
        Statement stDrop = con.createStatement();
        String sqlDrop = "DROP Table if exists v2h;";
        stDrop.execute(sqlDrop);

        //Create horizontal table
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE v2h (Oid int, ");
        ArrayList<String> attributeNames = new ArrayList<>();

        Statement stm = con.createStatement();
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append("SELECT distinct Key FROM " + tableName);
        ResultSet rs = stm.executeQuery(sbuilder.toString());
        while (rs.next()) {
            attributeNames.add(rs.getString(1));
        }
        Collections.sort(attributeNames);

        for (int i = 0; i < attributeNames.size(); i++) {
            String att = attributeNames.get(i);
            if (i % 2 == 0) {
                sb.append(att + " int");
            } else {
                sb.append(att + " varchar(255)");
            }
            if (i < attributeNames.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        System.out.printf(sb.toString());
        Statement st = con.createStatement();
        st.execute(sb.toString());




        //Fill horizontal table
        Statement getOid = con.createStatement();
        StringBuilder stb = new StringBuilder();
        stb.append("SELECT distinct Oid from " + tableName);
        ResultSet rset = getOid.executeQuery(stb.toString()); // number of tuples in horizontal table
        List<String> oids = new ArrayList<>();
        while (rset.next()){
            String oid = rset.getString(1);
            oids.add(oid);
        }

        HashMap<String, HashMap<String, String>> attributes = new HashMap<>(); //map to safe every value of each attribute

        for (int i = 0;  i < attributeNames.size(); i++) {
            StringBuilder builder = new StringBuilder();
            String attName = "a" + i;
            Statement getAttValues = con.createStatement();

            builder.append("Select Oid, val From " + tableName + "Where key = 'a" + i + "'");
            ResultSet results = getAttValues.executeQuery(getAttValues.toString());
            HashMap<String, String> attributeValues = new HashMap<>();

            while (results.next()) {

                attributeValues.put(results.getString(1), results.getString((2))); //To Do
            }
            attributes.put(attName, attributeValues);
        }

        int Oid = 1;

        for(int i = 0; i<oids.size(); i++) {
            StringBuilder build = new StringBuilder();
            build.append("INSERT into " + tableName + "VALUES(");

            for(int j = 0; j < attributes.size() ;j++) {
                String attName = "a" + i;
                HashMap<String, String> attributeVals = attributes.get(attName);

                if(attributeVals.containsKey(Oid)){
                    String value = attributeVals.get(Oid);
                    build.append(value);
                }
                else {
                    build.append("NULL");
                }

                if (i < oids.size() - 1) {
                    build.append(", ");
                }
            }
            sb.append(");");
            Statement insertAttValues = con.createStatement();
            insertAttValues.execute(build.toString());
        }
    }






    //Phase 1

    public static void generate(int num_tuples, double sparsity, int num_attributes) throws Exception {
        //delete H if exists
        Statement st = con.createStatement();
        String sql = "DROP Table if exists H;";
        st.execute(sql);

        //create Table with String and int Attributes
        String[] attributes = new String[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            attributes[i] = (i % 2 == 0)? "int" : "String";
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE H (");
        sb.append("Oid INT, ");
        for (int i = 1; i < num_attributes; i++) {
            if (attributes[i].equals("String")) {
                sb.append("a" + i + " VARCHAR(20)");
            } else {
                sb.append("a" + i + " INT");
            }
            if (i < num_attributes - 1) {
                sb.append(", ");
            } else {
                sb.append(");");
            }
        }
        Statement createTable = con.createStatement();
        createTable.execute(sb.toString());

        //Fill Table
        StringBuilder insert = new StringBuilder("INSERT INTO H VALUES ");
        int oid = 1;

        for (int i = 0; i < num_tuples - 1; i++) {
            insert.append("(");
            for (int j = 0; j < num_attributes; j++) {
                if (j == 0) {
                    insert.append(oid);
                    oid++;
                } else {
                    if (RANDOM.nextDouble() <= sparsity) {
                        insert.append("NULL");
                    } else {
                        String att = "a" + j;
                        if (attributes[j].equals("String")) {
                            String valueString = getStringValue(att, stringValues);
                            insert.append("'" + valueString + "'");
                        } else {
                            int valueInt = getIntValue(att, intValues);
                            insert.append(valueInt);
                        }
                    }
                }
                if (j != num_attributes - 1) {
                    insert.append(", ");
                }
            }
            if (i < num_tuples - 2) {
                insert.append("), ");
            }
        }
        insert.append(");");

        Statement insertInto = con.createStatement();
        insertInto.execute(insert.toString());

        //Insert special case with all attributes NULL
        StringBuilder insertSpecial = new StringBuilder("INSERT INTO H VALUES (");

        for(int i = 0; i < num_attributes; i++) {
            if (i == 0)
                insertSpecial.append(oid);
            else
                insertSpecial.append("NULL");
            if (i != num_attributes - 1) {
                insertSpecial.append(", ");
            }
        }
        insertSpecial.append(");");

        Statement insertSpecialInto = con.createStatement();
        insertSpecialInto.execute(insertSpecial.toString());

        System.out.println("Table created and filled.");
    }

    public static String getStringValue(String att, List<String> valueList) throws Exception{
        if(valueList.isEmpty())
            throw new Exception("All values already used 5 times in attribute " + att);

        String val = valueList.get(RANDOM.nextInt(valueList.size()));   //choose random value from list

        stringValueCounters = stringAttributeRegister.getOrDefault(att, new HashMap<>());  //get counters of attribute
        int counter = stringValueCounters.getOrDefault(val, 0);    //get how often the value has been used
        int updatedCounter = counter + 1;                                    //increase by one
        stringValueCounters.put(val, updatedCounter);                        //update counter in map
        stringAttributeRegister.put(att, stringValueCounters);               //update register

        if(updatedCounter <= 5)                                              //check if it would be the fifth time, if so choose other value
            return val;
        else {
            List<String> updatedValueList = new ArrayList<>(valueList);
            updatedValueList.remove(val);
            return getStringValue(att, updatedValueList);                    //check again
        }
    }


    public static int getIntValue(String att, List<Integer> valueList) throws Exception{
        if(valueList.isEmpty())
            throw new Exception("All values used more than 5 times in attribute " + att);

        int val = valueList.get(RANDOM.nextInt(valueList.size()));   //choose random value from list

        intValueCounters = intAttributeRegister.getOrDefault(att, new HashMap<>());  //get counters of attribute
        int counter = intValueCounters.getOrDefault(val, 0);    //get how often the value has been used
        int updatedCounter = counter + 1;                                 //increase by one
        intValueCounters.put(val, updatedCounter);                        //update counter in map
        intAttributeRegister.put(att, intValueCounters);                  //update register

        if(updatedCounter <= 5)                                           //check if it would be the fifth time, if so choose other value
            return val;
        else {
            List<Integer> updatedValueList = new ArrayList<>(valueList);
            updatedValueList.removeAll(Arrays.asList(val));
            return getIntValue(att, updatedValueList);                   //check again
        }
    }

    public static void showConnectionAndSQL() throws SQLException {
        Statement st = con.createStatement();
        String delete = "DROP Table if exists showCon;";
        st.execute(delete);

        Statement stm = con.createStatement();
        String create = "CREATE TABLE showCon (Oid INT, a1 VARCHAR(5));";
        stm.execute(create);

        Statement stmnt = con.createStatement();
        String insert = "INSERT INTO showCon VALUES (1, 'abc');";
        stmnt.execute(insert);

        System.out.println("Table created and filled.");
    }




    //Phase 2

    public static void h2v(String tableName) throws SQLException {
        Statement stDrop = con.createStatement();
        String sqlDrop = "DROP Table if exists h2v_temp;";
        stDrop.execute(sqlDrop);
        Statement orginalDrop = con.createStatement();
        String sqlOrginalDrop = "DROP Table if exists h2v;";
        orginalDrop.execute(sqlOrginalDrop);


        Statement stCreateVertical = con.createStatement();
        String sqlCreateVertical = "CREATE TABLE H2V_temp (Oid int, Key varchar(5), Val varchar(255));";
        stCreateVertical.execute(sqlCreateVertical);

        DatabaseMetaData metaData = con.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, tableName, null);

        Map<String, String> attributeTypes = new HashMap<>();

        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            String dataType = resultSet.getString("TYPE_NAME");
            attributeTypes.put(columnName, dataType);
        }

        for (String s : attributeTypes.keySet()) {
            if (!s.equals("oid")) {
                String sql = "SELECT oid," + s + " FROM " + tableName + " WHERE " + s + " is not null;";
                System.out.println(sql);
                Statement st = con.createStatement();
                ResultSet rs1 = st.executeQuery(sql);
                while (rs1.next()) {
                    int oidValue = rs1.getInt("oid");
                    String value = rs1.getString(s);
                    StringBuilder insert = new StringBuilder("INSERT INTO H2V_temp VALUES ( ");
                    insert.append(oidValue + ", '" + s + "', '" + value + "');");
                    Statement stInsertVertical = con.createStatement();
                    stInsertVertical.execute(insert.toString());
                }
            }
        }


        String sqlSortTable = "CREATE TABLE H2V AS SELECT * FROM H2V_temp ORDER BY Oid ASC, key;";
        Statement stSortTable = con.createStatement();
        stSortTable.execute(sqlSortTable);

        String sqlDropTemp = "DROP TABLE H2V_temp;";
        Statement stDropTemp = con.createStatement();
        stDropTemp.execute(sqlDropTemp);
    }

}