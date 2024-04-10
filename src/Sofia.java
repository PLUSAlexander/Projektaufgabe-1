import java.sql.*;
import java.util.*;

//Projektaufgabe 1

public class Sofia {
    private static String url = "jdbc:postgresql://localhost/postgres";
    private static String user = "postgres";
    private static String pwd = "1234";
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
        generate(7,0.5,4);
        h2v("h");
        v2h("hh2v");
    }



    //Phase 1

    public static void generate(int num_tuples, double sparsity, int num_attributes) throws Exception {
        //delete H if it already exists
        Statement st = con.createStatement();
        String sql = "DROP Table if exists H;";
        st.execute(sql);


        //create table with String and int attributes
        String[] attributes = new String[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            attributes[i] = (i % 2 == 0) ? "int" : "String";
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

        Random rand = new Random(14);

        //fill table
        StringBuilder insert = new StringBuilder("INSERT INTO H VALUES ");
        int oid = 1;

        for (int i = 0; i < num_tuples - 1; i++) {
            insert.append("(");
            for (int j = 0; j < num_attributes; j++) {
                if (j == 0) {
                    insert.append(oid);
                    oid++;
                } else {
                    if (rand.nextDouble() <= sparsity) {
                        insert.append("NULL");
                    } else {
                        String att = "a" + j;
                        if (attributes[j].equals("String")) {
                            String valueString = getStringValue(att, stringValues);
                            insert.append("'").append(valueString).append("'");
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


        //insert special case with all attributes NULL
        StringBuilder insertSpecial = new StringBuilder("INSERT INTO H VALUES (");

        for (int i = 0; i < num_attributes; i++) {
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


    public static String getStringValue(String att, List<String> valueList) throws Exception {
        if (valueList.isEmpty())
            throw new Exception("All values already used 5 times in attribute " + att);

        String val = valueList.get(RANDOM.nextInt(valueList.size()));   //choose random value from list

        stringValueCounters = stringAttributeRegister.getOrDefault(att, new HashMap<>());  //get counters of attribute
        int counter = stringValueCounters.getOrDefault(val, 0);    //get how often the value has been used
        int updatedCounter = counter + 1;                                    //increase by one
        stringValueCounters.put(val, updatedCounter);                        //update counter in map
        stringAttributeRegister.put(att, stringValueCounters);               //update register

        if (updatedCounter <= 5)                                              //check if it would be the fifth time, if so choose other value
            return val;
        else {
            List<String> updatedValueList = new ArrayList<>(valueList);
            updatedValueList.remove(val);
            return getStringValue(att, updatedValueList);                    //check again
        }
    }


    public static int getIntValue(String att, List<Integer> valueList) throws Exception {
        if (valueList.isEmpty())
            throw new Exception("All values used more than 5 times in attribute " + att);

        int val = valueList.get(RANDOM.nextInt(valueList.size()));   //choose random value from list

        intValueCounters = intAttributeRegister.getOrDefault(att, new HashMap<>());  //get counters of attribute
        int counter = intValueCounters.getOrDefault(val, 0);    //get how often the value has been used
        int updatedCounter = counter + 1;                                 //increase by one
        intValueCounters.put(val, updatedCounter);                        //update counter in map
        intAttributeRegister.put(att, intValueCounters);                  //update register

        if (updatedCounter <= 5)                                           //check if it would be the fifth time, if so choose other value
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
        String create = "CREATE TABLE showCon(Oid INT, a1 VARCHAR(5));";
        stm.execute(create);

        Statement stmnt = con.createStatement();
        String insert = "INSERT INTO showCon VALUES(1, 'abc');";
        stmnt.execute(insert);

        System.out.println("Table created and filled.");
    }




    //Phase 2

    public static void h2v(String tableName) throws SQLException {
        //delete h2v and h2v_temp if they already exist
        Statement stDrop = con.createStatement();
        String sqlDrop = "DROP Table if exists h2v_temp;";
        stDrop.execute(sqlDrop);
        Statement orginalDrop = con.createStatement();
        String sqlOrginalDrop = "DROP Table if exists " + tableName + "H2V;";
        orginalDrop.execute(sqlOrginalDrop);


        //create vertical table
        Statement stCreateVertical = con.createStatement();
        String sqlCreateVertical = "CREATE TABLE H2V_temp (Oid int, Key varchar(5), Val varchar(255));";
        stCreateVertical.execute(sqlCreateVertical);


        //get data from table
        DatabaseMetaData metaData = con.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, tableName, null);

        Map<String, String> attributeTypes = new HashMap<>();

        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            String dataType = resultSet.getString("TYPE_NAME");
            attributeTypes.put(columnName, dataType);
        }


        //fill table
        for (String s : attributeTypes.keySet()) {
            if (!s.equals("oid")) {
                String sql = "SELECT oid," + s + " FROM " + tableName + " WHERE " + s + " is not null;";
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


        //sort table
        String sqlSortTable = "CREATE TABLE " + tableName + "H2V AS SELECT * FROM H2V_temp ORDER BY Oid ASC, key;";
        Statement stSortTable = con.createStatement();
        stSortTable.execute(sqlSortTable);

        String sqlDropTemp = "DROP TABLE H2V_temp;";
        Statement stDropTemp = con.createStatement();
        stDropTemp.execute(sqlDropTemp);

        System.out.println("Successfully converted to vertical.");
    }


    public static void v2h(String tableName) throws SQLException {
        //delete Horizontal table and v2helper if they already exist
        Statement stmDrop = con.createStatement();
        String sqlDrop = "DROP Table if exists " + tableName + "V2H;";
        stmDrop.execute(sqlDrop);
        Statement stDrop = con.createStatement();
        String stringDrop = "DROP Table if exists v2helper;";
        stDrop.execute(stringDrop);


        //get attribute names
        Statement stmGetAttNames = con.createStatement();
        String getAttNames = "SELECT distinct Key FROM " + tableName;
        ArrayList<String> attributeNames = new ArrayList<>();
        ResultSet rs = stmGetAttNames.executeQuery(getAttNames);
        while (rs.next()) {
            attributeNames.add(rs.getString(1));
        }
        Collections.sort(attributeNames);


        //create helper table for joins and fill initially with all oids (as integers)
        Statement stmJoinHelper = con.createStatement();
        String joinHelper = "CREATE table v2helper (oid int);";
        stmJoinHelper.execute(joinHelper);

        Statement stMaxOid = con.createStatement();
        String maxOidString = "SELECT MAX(oid) AS max_value FROM " + tableName + ";";
        ResultSet rsMaxOid = stMaxOid.executeQuery(maxOidString);
        int maxOid = 0;
        while (rsMaxOid.next()) {
            maxOid = Integer.parseInt(rsMaxOid.getString(1));}

        for (int i = 1; i <= maxOid; i++) {
            Statement stmInsertHelper = con.createStatement();
            String insertHelper = "insert into v2helper values(" + i + ");";
            stmInsertHelper.execute(insertHelper);
        }


        //create temporary table for each attribute
        int attCounter = 0;

        for (String attName : attributeNames) {
            Statement stmGetAttOid = con.createStatement();       //get oids from all tuples that have the attribute
            String getAttOid = "SELECT oid FROM " + tableName + " WHERE key = '" + attName + "';";
            ResultSet resultOid = stmGetAttOid.executeQuery(getAttOid);

            Statement stmGetAttVal = con.createStatement();      //get values from all tuples that have the attribute
            String getAttVal = "SELECT val FROM " + tableName + " WHERE key = '" + attName + "';";
            ResultSet resultVal = stmGetAttVal.executeQuery(getAttVal);

            Statement stmCreateHelper = con.createStatement();
            String createHelper;
            if(attCounter % 2 == 0){       //adjust type (String or Integer)
                createHelper = "CREATE TEMPORARY TABLE " + attName + " (oid int, " + attName + " varchar(255));";
            }
            else {
                createHelper = "CREATE TEMPORARY TABLE " + attName + " (oid int, " + attName + " int);";
            }
            stmCreateHelper.execute(createHelper);

            while (resultOid.next() && resultVal.next()) {
                Statement stmFillHelper = con.createStatement();
                String oidString = resultOid.getString(1);
                int oid = Integer.parseInt(oidString);
                String valString = resultVal.getString(1);
                if(attCounter % 2 == 0){
                    String fillHelper = "insert into " + attName + "(oid, " + attName + ") values(" + oid + ", '" + valString + "');";
                    stmFillHelper.executeUpdate(fillHelper);
                }
                else {
                    int val = Integer.parseInt(valString);
                    String fillHelper = "insert into " + attName + "(oid, " + attName + ") values(" + oid + ", " + val + ");";
                    stmFillHelper.executeUpdate(fillHelper);
                }
            }
            attCounter++;


            //join attribute with the helper and update the helper
            Statement stmCreateV3helper = con.createStatement();
            String createV3helper = "CREATE TABLE v3helper AS (SELECT * FROM v2helper);";
            stmCreateV3helper.executeUpdate(createV3helper);

            Statement stmDeleteOldV2helper = con.createStatement();
            String deleteOldV2helper = "DROP Table if exists v2helper;";
            stmDeleteOldV2helper.executeUpdate(deleteOldV2helper);

            Statement stmJoinAttOfTuple = con.createStatement();
            String joinAttOfTuple = "CREATE TABLE v2helper AS (SELECT v3helper.*, " + attName + "." + attName + " FROM v3helper LEFT JOIN " + attName + " ON v3helper.oid = " + attName + ".oid);";
            stmJoinAttOfTuple.executeUpdate(joinAttOfTuple);

            Statement stmDeleteOldV3helper = con.createStatement();
            String deleteOldV3helper = "DROP Table if exists v3helper;";
            stmDeleteOldV3helper.executeUpdate(deleteOldV3helper);
        }


        //sort table
        String sqlSortTable = "CREATE TABLE " + tableName + "V2H AS SELECT * FROM v2helper ORDER BY oid ASC;";
        Statement stSortTable = con.createStatement();
        stSortTable.execute(sqlSortTable);

        String sqlDropTemp = "DROP TABLE v2helper;";
        Statement stDropTemp = con.createStatement();
        stDropTemp.execute(sqlDropTemp);

        System.out.println("Successfully converted to horizontal.");
    }
}