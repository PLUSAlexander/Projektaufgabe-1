import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//Projektaufgabe_1
public class Main {

    private static final Random RANDOM = new Random(42);
    private static String url = "jdbc:postgresql://localhost/postgres";
    private static String user = "postgres";
    private static String pwd = "1234";
    private static Connection con;

    public static void main(String[] args) throws SQLException {
        con = DriverManager.getConnection(url, user, pwd);
        /*
        for (int i = 1; i <= 100; i++) {
            generate(i, i/100, i);   // probiere verschiedene werte um die korrektheit zu testen
        } */

        /*generate(1001, 0.5, 12);
        h2v("h"); // ACHTUNG: muss klein geschrieben werden!!!
        v2h_view("h_h2v", false);
        q_i("h_h2v_v2h");
        q_ii("h_h2v_v2h", false);*/


        benchmark();
        //showConnectionAndSQL();
        con.close();
    }



    // v2h as view ->
    public static void v2h_view(String tableName, boolean index) throws SQLException {
        //delete view if it already exists
        Statement stmDrop = con.createStatement();
        String sqlDrop = "DROP VIEW if exists " + tableName + "_V2H;";
        stmDrop.execute(sqlDrop);

        //get attribute names
        Statement stmGetAttNames = con.createStatement();
        String getAttNames = "SELECT distinct Key FROM " + tableName;
        ArrayList<String> attributeNames = new ArrayList<>();
        ResultSet rs = stmGetAttNames.executeQuery(getAttNames);
        while (rs.next()) {
            attributeNames.add(rs.getString(1));
        }
        Collections.sort(attributeNames, new NaturalOrderComparator());

        //get int attribute names
        Statement stmGetIntAttNames = con.createStatement();
        String getIntAttNames = "SELECT distinct Key FROM " + tableName + "_int";
        ArrayList<String> intAttributeNames = new ArrayList<>();
        ResultSet rsInt = stmGetIntAttNames.executeQuery(getIntAttNames);
        while (rsInt.next()) {
            intAttributeNames.add(rsInt.getString(1));
        }


        //get String attribute names
        Statement stmGetStringAttNames = con.createStatement();
        String getStringAttNames = "SELECT distinct Key FROM " + tableName + "_string";
        ArrayList<String> stringAttributeNames = new ArrayList<>();
        ResultSet rsString = stmGetStringAttNames.executeQuery(getStringAttNames);
        while (rsString.next()) {
            stringAttributeNames.add(rsString.getString(1));
        }





        //create view
        Statement createViewStm = con.createStatement();
        StringBuilder createView = new StringBuilder("CREATE VIEW " + tableName + "_V2H AS SELECT oid.oid, ");
        if (index) {
            createView = new StringBuilder("CREATE MATERIALIZED VIEW " + tableName + "_V2H AS SELECT oid.oid, "); // MATERIALISIERUNG!
        }

        for(int i = 0; i <= attributeNames.size()-1; i++){
            String value = attributeNames.get(i);
            if(!value.equals("alle")) {
                createView.append(value + ".val AS " + value);
                if (i <= attributeNames.size() - 3) {
                    createView.append(", "); }
            }
        }

        createView.append(" FROM ((SELECT distinct oid FROM " + tableName + ") AS oid LEFT JOIN ");

        for (int i = 0; i <= attributeNames.size() - 1; i++) {
            String attName = attributeNames.get(i);
            if (!attName.equals("alle")) {
                if(intAttributeNames.contains(attName)) {
                    createView.append("(SELECT * FROM " + tableName + "_int WHERE " + tableName + "_int.key = '" + attName + "') AS " + attName + " ON oid.oid = " + attName + ".oid ");
                }
                else {
                    createView.append("(SELECT * FROM " + tableName + "_string WHERE " + tableName + "_string.key = '" + attName + "') AS " + attName + " ON oid.oid = " + attName + ".oid ");
                }
                if (i <= attributeNames.size() - 3) {
                    createView.append("LEFT JOIN ");
                }
            }
        }
        createView.append(") ORDER BY oid.oid");
        createViewStm.execute(createView.toString());

        if (index) {
            Statement indexStm = con.createStatement();
            String createIndex = "CREATE INDEX idx_oid_hash ON h_h2v_v2h USING hash (oid);";  // WITH INDEX!!!
            indexStm.execute(createIndex);
        }
        //System.out.println("Successfully converted to horizontal.");
    }

    public static void h2v(String tableName) throws SQLException {
        //create table for int attributes
        Statement intDrop = con.createStatement();
        String sqlintDrop = "DROP table if exists " + tableName + "_h2v_int cascade;";
        intDrop.execute(sqlintDrop);

        Statement stCreateIntVertical = con.createStatement();
        String sqlCreateIntVertical = "CREATE TABLE " + tableName + "_h2v_int (Oid int, Key varchar(20), Val int);";
        stCreateIntVertical.execute(sqlCreateIntVertical);


        //create table for String attributes
        Statement StringDrop = con.createStatement();
        String sqlStringDrop = "DROP table if exists " + tableName + "_h2v_string cascade;";
        StringDrop.execute(sqlStringDrop);

        Statement stCreateStringVertical = con.createStatement();
        String sqlCreateStringVertical = "CREATE TABLE " + tableName + "_h2v_string (Oid int, Key varchar(20), Val varchar(255));";
        stCreateStringVertical.execute(sqlCreateStringVertical);

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
        List<Integer> insertedOids = new ArrayList<>();

        for (String s : attributeTypes.keySet()) {
            if (!s.equals("oid")) {
                String sql = "SELECT oid," + s + " FROM " + tableName + " WHERE " + s + " is not null;";
                Statement st = con.createStatement();
                ResultSet rs1 = st.executeQuery(sql);
                if (attributeTypes.get(s).equals("int4")) {
                    while (rs1.next()) {
                        int oidValue = rs1.getInt("oid");
                        int value = rs1.getInt(s);
                        String insert = "INSERT INTO " + tableName + "_h2v_int VALUES ( " + oidValue + ", '" + s + "', '" + value + "');";
                        Statement stInsertVertical = con.createStatement();
                        stInsertVertical.execute(insert);
                        if (!insertedOids.contains(oidValue)) {
                            insertedOids.add(oidValue);
                        }
                    }
                }
                else {
                    while (rs1.next()) {
                        int oidValue = rs1.getInt("oid");
                        String value = rs1.getString(s);
                        String insert = "INSERT INTO " + tableName + "_h2v_string VALUES ( " + oidValue + ", '" + s + "', '" + value + "');";
                        Statement stInsertVertical = con.createStatement();
                        stInsertVertical.execute(insert);
                        if (!insertedOids.contains(oidValue)) {
                            insertedOids.add(oidValue);
                        }
                    }
                }
            }
        }


        //check for special case
        Statement countTuplesStm = con.createStatement();
        String countTuples = "SELECT COUNT(*) as tupleCount FROM " + tableName;
        ResultSet amountTuples = countTuplesStm.executeQuery(countTuples);

        int numberTuples = 0;
        while (amountTuples.next()){
            numberTuples = amountTuples.getInt("tupleCount");}

        List<Integer> allOids = new ArrayList<>();
        for(int i = 1; i <= numberTuples; i++){
            allOids.add(i);
        }

        for(int i = 1; i <= numberTuples; i++){
            if(insertedOids.contains(i)){
                allOids.remove(Integer.valueOf(i));
            }
        }

        for (int nullOid : allOids) {
            Statement insertAllNullStm = con.createStatement();
            String insertAllNull = "INSERT INTO " + tableName + "_h2v_string VALUES ( " + nullOid + ", 'alle');";
            insertAllNullStm.execute(insertAllNull);
        }


        //sort table
        //delete vertical view h2v if it already exists
        Statement orginalDrop = con.createStatement();
        String sqlOrginalDrop = "DROP view if exists " + tableName + "_h2v cascade;";
        orginalDrop.execute(sqlOrginalDrop);


        //create vertical view h2v
        Statement stCreateVertical = con.createStatement();
        String sqlCreateVertical = "CREATE materialized view " + tableName + "_h2v AS SELECT oid, key, CAST(val AS VARCHAR) FROM " + tableName + "_h2v_int UNION ALL SELECT oid, key, val FROM " + tableName + "_h2v_string order by oid, key asc;";
        stCreateVertical.execute(sqlCreateVertical);

        Statement indexStm = con.createStatement();
        String createIndex = "CREATE INDEX idx_oid_h_h2v ON h_h2v (oid);";  // WITH INDEX!!!
        indexStm.execute(createIndex);

        Statement clusterStm = con.createStatement();
        String createCluster = "ALTER TABLE h_h2v CLUSTER ON idx_oid_h_h2v;";  // CLUSTER!!!, good for equality (left) join
        clusterStm.execute(createCluster);

        //System.out.println("Successfully converted to vertical.");
    }

    public static void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
        if (num_attributes <= 0 || num_tuples <= 0 || num_attributes > 1600 || sparsity > 1.0 || sparsity < 0.0) {
            throw new SQLException("generate() args wrong!!!");
        }
        Statement st = con.createStatement();
        String sql = "DROP Table if exists H CASCADE;";
        st.execute(sql);

        String[] attributs = new String[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            attributs[i] = (i % 2 == 0)? "int" : "String";
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE H (");
        for (int i = 0; i < num_attributes; i++) {
            if (i == 0) {
                sb.append("Oid INT ");
            } else {
                if (attributs[i].equals("String")) {
                    sb.append("a" + i + " VARCHAR(255)");
                } else {
                    sb.append("a" + i + " INT");
                }
            }
            if (i < num_attributes - 1) {
                sb.append(", ");
            } else {
                sb.append(");");
            }
        }
        Statement createTable = con.createStatement();
        createTable.execute(sb.toString());


        Map<String, Map<String, Integer>> attributeCounts = new HashMap<>();
        int oid = 1;
        StringBuilder insert = new StringBuilder("INSERT INTO H VALUES ");
        for (int i = 0; i < num_tuples; i++) {
            insert.append("(");
            for (int j = 0; j < num_attributes; j++) {
                if (j == 0) {
                    insert.append(oid);
                    oid++;
                } else {
                    if (RANDOM.nextDouble() <= sparsity) {
                        insert.append("NULL");
                    } else {
                        String attributeValue;
                        if (attributs[j].equals("String")) {
                            attributeValue = generateRandomString(1, 20);
                        } else {
                            attributeValue = Integer.toString(RANDOM.nextInt(num_tuples / 5));
                        }

                        Map<String, Integer> columnCounts = attributeCounts.computeIfAbsent("a" + j, k -> new HashMap<>());
                        int count = columnCounts.getOrDefault(attributeValue, 0);
                        if (count >= 5) {
                            insert.append("NULL");
                        } else {
                            insert.append("'" + attributeValue + "'");
                            columnCounts.put(attributeValue, count + 1);
                        }
                    }
                }
                if (j != num_attributes - 1) {
                    insert.append(", ");
                }
            }
            if (i < num_tuples - 1) {
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


        //System.out.println("Table created and filled.");

    }



    public static String generateRandomString(int minLength, int maxLength) {
        int length = minLength + RANDOM.nextInt(Math.min(maxLength - minLength + 1, 21)); // Maximale Länge auf 21 begrenzen, um sicherzustellen, dass die resultierende Länge nicht größer als 20 ist
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = RANDOM.nextInt(52); // 52 Buchstaben im Alphabet (26 Großbuchstaben + 26 Kleinbuchstaben)
            char randomChar = (char)('A' + RANDOM.nextInt(26)); // Zufälliger Großbuchstabe
            if (randomIndex >= 26) {
                randomChar = (char)('a' + RANDOM.nextInt(26)); // Zufälliger Kleinbuchstabe
            }
            sb.append(randomChar);
        }
        return sb.toString();
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

    public static void benchmark() throws SQLException {
        double exponent = 1.7;
        int minDatensatz = 1001; //1001
        int numAttributs = 3; //5
        double sparsity = 1;

        int totalQueries = 0;
        int queriesThisMinute = 0;
        ArrayList<String> perMinuteQueries = new ArrayList<>();

        long start = System.currentTimeMillis();
        long executionTime = 0;
        long nextMinute = start + 60000;
        int z = 1;
        String tableSizeV = "SELECT pg_size_pretty(pg_total_relation_size('h_h2v'));";
        String tableSizeH = "SELECT pg_size_pretty(pg_total_relation_size('h'));";
        Random rand = new Random();

        for (int i = numAttributs; i <= 15; i += 3) {
            for (int j = minDatensatz; j < 10000; j *= exponent) { //exponentiell!
                for (double x = sparsity; x <= 6; x += 1.0) {
                    generate(j, Math.pow(2, -x), i);
                    h2v("h"); // ACHTUNG: muss klein geschrieben werden!!!
                    v2h_view("h_h2v", true); //H2V VERBESSERUNG AUSSCHALTEN!!!
                    q_i("h_h2v_v2h");
                    q_ii("h_h2v_v2h", true);
                    /*
                    Statement stVSize = con.createStatement();
                    Statement stHSize = con.createStatement();
                    ResultSet rs1 = stVSize.executeQuery(tableSizeV);
                    ResultSet rs12 = stHSize.executeQuery(tableSizeH);
                    while (rs1.next() && rs12.next()) {
                        System.out.println("Speicherverbrauch V: " + rs1.getString(1)); //SPEICHERVERBRAUCH
                        System.out.println("Speicherverbrauch H: " + rs12.getString(1)); //SPEICHERVERBRAUCH
                    } */

                    int k = 1;
                    long startInner = System.currentTimeMillis();
                    while (k <= 100) {
                        Statement st = con.createStatement();
                        String q_i = "select * from q_i(" + rand.nextInt(j) + ");";  // genau ein Resultat
                        //String sql1 = "select * from h_h2v_v2h where oid = " + rand.nextInt(j) + ";";  // genau ein Resultat
                        ResultSet rs = st.executeQuery(q_i);

                        int randomNumber = rand.nextInt(i - 2) + 2;
                        if (randomNumber % 2 != 0) {
                            randomNumber--;

                        }
                        Statement st2 = con.createStatement();
                        //String sql2 = "Select oid from h_h2v_v2h where a" + randomNumber + " = '" + rand.nextInt(j/5) + "';"; // ca. 5 Resultate
                        String q_ii = "select oid from q_ii(\'a" + randomNumber + "\', " + rand.nextInt(j / 5) + ");";
                        ResultSet rs2 = st2.executeQuery(q_ii);/*
                        int counter = 0;
                        while (rs2.next()) {
                            counter++;
                        }
                        //System.out.println(counter); //ZEIGEN, DASS 5x VORKOMMT
                        */
                            k += 2;
                            totalQueries += 2;
                            queriesThisMinute += 2;
                    }
                    long endTimeInner = System.currentTimeMillis();
                    long executionTimeInner = endTimeInner - startInner;
                    //executionTime += executionTimeInner;
                    System.out.println("For num_tuples: num_tuples: " + j + ", sparsity: " + Math.pow(2, -x) + ", num_attributes: " + i + " ||| Throughtput: " + ((double) k / (double) executionTimeInner) * 1000.0 + " queries/Sek.");
                    //System.out.println("num_tuples: " + j + ", sparsity: " + Math.pow(2, -x) + ", num_attributes: " + i);

                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= nextMinute) {
                        perMinuteQueries.add("Queries in Min. " + z + ": " + queriesThisMinute);
                        nextMinute += 60000;
                        queriesThisMinute = 0;
                        z++;
                    }
                }
            }
        }
        System.out.println("\n\n\n");

        for (String s : perMinuteQueries) {
            System.out.println(s);
        }

        long endTime = System.currentTimeMillis();
        executionTime = endTime - start;
        System.out.println("gesamte Ausführungszeit in Min.: " + (double) executionTime/60000.0 + " ||| in total: " + totalQueries + " Queries");
        System.out.println("Anfragen pro Minute: " + (double) totalQueries/((double) executionTime/60000.0));
    }



    public static void q_i (String horizontalTable) throws SQLException {

        // Select * from q_i(42)
        // Select * from H_VIEW where oid = ???
        Statement StdropFunc = con.createStatement();
        String dropFunc = "drop function if exists q_i(inputOid int)";
        StdropFunc.execute(dropFunc);

        Statement createQ_iStm = con.createStatement();
        StringBuilder createQ_i = new StringBuilder("CREATE OR REPLACE FUNCTION q_i(inputOid int) RETURNS TABLE(oid int, ");

        DatabaseMetaData metaData = con.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, horizontalTable, null);


        /*
        int i = 1;
        //Map<String, String> attributeTypes = new HashMap<>();
        ArrayList<String> attributeNames = new ArrayList<>();
        ArrayList<String> attType = new ArrayList<>();
        while (resultSet.next()) {
            if (i % 2 == 0) {
                attType.add("int4");
            } else {
                attType.add("varchar(255)");
            }
            String columnName = resultSet.getString("COLUMN_NAME");
            attributeNames.add(columnName);
            //String dataType = resultSet.getString("TYPE_NAME");
            //attributeTypes.put(columnName, dataType);
            i++;
        }
        Collections.sort(attributeNames, new NaturalOrderComparator());

        int attCounter = 0;


        for(String s : attributeTypes.keySet()){
            if (!s.equals("oid")) {
                if (attributeTypes.get(s).equals("int4")) {
                    createQ_i.append(s).append(" int");
                } else {
                    createQ_i.append(s).append(" varchar(255)");
                }
                attCounter++;
                if (attCounter <= attributeTypes.size() - 2) {
                    createQ_i.append(", ");
                }
            }
        }
         */

        int i = 1;
        ArrayList<String> attributeNames = new ArrayList<>();
        ArrayList<String> attType = new ArrayList<>();
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            attributeNames.add(columnName);
            if (i % 2 == 0) {
                attType.add("int4");
            } else {
                attType.add("varchar(255)");
            }
            i++;
        }
        Collections.sort(attributeNames, new NaturalOrderComparator());

        int attCounter = 0;
        for (String attributeName : attributeNames) {
            String dataType = attType.get(attCounter); // Holen Sie den Datentyp für das aktuelle Attribut
            if (!attributeName.equals("oid")) {
                if (dataType.equals("int4")) {
                    createQ_i.append(attributeName).append(" int");
                } else {
                    createQ_i.append(attributeName).append(" varchar(255)");
                }
                attCounter++;
                if (attCounter < attributeNames.size() - 1) {
                    createQ_i.append(", ");
                }
            }
        }
        //createQ_i.append(") AS $$ BEGIN RETURN QUERY EXECUTE 'SELECT * FROM h_h2v_v2h WHERE h_h2v_v2h.oid = $1' USING inputOid; END; $$ LANGUAGE plpgsql;");


        //BEGIN RETURN QUERY select * from h_h2v_v2h where h_h2v_v2h.oid = input_oid; END; $$ language plpgsql;

        createQ_i.append(") AS $$ BEGIN RETURN QUERY EXECUTE 'SELECT * FROM h_h2v_v2h WHERE h_h2v_v2h.oid = $1' USING inputOid; END; $$ LANGUAGE plpgsql;");

        createQ_iStm.execute(createQ_i.toString());
        /*
        Statement StmTryQ_i = con.createStatement();
        String tryQ_i = "SELECT * from q_i(5)";
        ResultSet rs = StmTryQ_i.executeQuery(tryQ_i);
        while (rs.next()) {
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            System.out.println(rs.getString(3));
            System.out.println(rs.getString(4));
            System.out.println(rs.getString(5));
            System.out.println(rs.getString(6));
            System.out.println(rs.getString(7));
        } */
    }

    public static void q_ii(String horizontalTable, boolean intAtt) throws SQLException {
        Statement StdropFunc = con.createStatement();
        String dropFunc = "drop function if exists q_ii(attname varchar(255), input_val integer)";
        StdropFunc.execute(dropFunc);

        Statement StdropStringFunc = con.createStatement();
        String dropStringFunc = "drop function if exists q_ii(attname varchar(255), input_val varchar(255))";
        StdropStringFunc.execute(dropStringFunc);

        Statement createQ_iStm = con.createStatement();
        StringBuilder createQ_i = new StringBuilder();
        if (intAtt) {
            createQ_i = new StringBuilder("CREATE OR REPLACE FUNCTION q_ii(attname varchar(255), input_val integer) RETURNS TABLE(oid int)");
        } else {
            createQ_i = new StringBuilder("CREATE OR REPLACE FUNCTION q_ii(attname varchar(255), input_val varchar(255)) RETURNS TABLE(oid int)");
        }
        /*
        DatabaseMetaData metaData = con.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, horizontalTable, null);

        Map<String, String> attributeTypes = new HashMap<>();

        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            String dataType = resultSet.getString("TYPE_NAME");
            attributeTypes.put(columnName, dataType);
        }

        int attCounter = 0;

        for(String s : attributeTypes.keySet()){
            if (!s.equals("oid")) {
                if (attributeTypes.get(s).equals("int4")) {
                    createQ_i.append(s).append(" int");
                } else {
                    createQ_i.append(s).append(" varchar(255)");
                }
                attCounter++;
                if (attCounter <= attributeTypes.size() - 2) {
                    createQ_i.append(", ");
                }
            }
        } */
        //AS $$ DECLARE sql_query text; BEGIN sql_query := 'SELECT oid FROM h_h2v_v2h WHERE ' || attribute_name || ' = $1'; RETURN QUERY EXECUTE sql_query USING input_val; END; $$ LANGUAGE plpgsql;
        createQ_i.append(" AS $$ DECLARE sql_query text; BEGIN sql_query := 'SELECT oid FROM h_h2v_v2h WHERE ' || attname || ' = $1'; RETURN QUERY EXECUTE sql_query USING input_val; END; $$ LANGUAGE plpgsql;");
        createQ_iStm.execute(createQ_i.toString());

        /*
        Statement StmTryQ_ii = con.createStatement();
        String tryQ_ii = "SELECT * from q_ii('a2', 4)";
        ResultSet rs = StmTryQ_ii.executeQuery(tryQ_ii);

        while (rs.next()) {
            System.out.println(rs.getInt(1));
        } */

    }








    static class NaturalOrderComparator implements Comparator<String> {
        private final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

        @Override
        public int compare(String s1, String s2) {
            // Check if either string is "alle", and if so, return accordingly
            if (s1.equals("alle")) {
                return 1; // "alle" should come after any other string
            } else if (s2.equals("alle")) {
                return -1; // "alle" should come after any other string
            }

            Matcher matcher1 = NUMBER_PATTERN.matcher(s1);
            Matcher matcher2 = NUMBER_PATTERN.matcher(s2);

            // Find the first number in each string
            while (matcher1.find() && matcher2.find()) {
                String match1 = matcher1.group();
                String match2 = matcher2.group();

                // Compare the numbers as integers
                int result = Integer.compare(Integer.parseInt(match1), Integer.parseInt(match2));
                if (result != 0) {
                    return result;
                }
            }

            // If one string has more numbers than the other, the one with more numbers comes later
            if (matcher1.find()) {
                return 1;
            } else if (matcher2.find()) {
                return -1;
            }

            // If no numbers found, perform lexicographical comparison
            return s1.compareTo(s2);
        }
    }


}