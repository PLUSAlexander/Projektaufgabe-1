import java.sql.*;
import java.util.*;
//Projektaufgabe_1
public class Main {

    private static final Random RANDOM = new Random();
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

        generate(3, 0.3, 4);

        h2v("h"); // ACHTUNG: muss klein geschrieben werden!!!
        v2h("h2v");
    }

    public static void v2h(String tableName) throws SQLException {
        Statement stDrop = con.createStatement();
        String sqlDrop = "DROP Table if exists v2h;";
        stDrop.execute(sqlDrop);

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

        for (int i = 0; i <= attributeNames.size() - 1; i++) {
            String att = attributeNames.get(i);
            if (i % 2 == 0) {
                sb.append(att + " varchar(255)");
            } else {
                sb.append(att + " int");
            }
            if (i < attributeNames.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        System.out.println(sb.toString());
        Statement st = con.createStatement();
        st.execute(sb.toString());

        String maxOidString = "SELECT MAX(oid) AS max_value FROM " + tableName + ";";
        Statement stMaxOid = con.createStatement();
        ResultSet rsMaxOid = stMaxOid.executeQuery(maxOidString);
        int maxOid = 0;
        while (rsMaxOid.next()) {
            maxOid = Integer.parseInt(rsMaxOid.getString(1));
        }
        Statement stTemp = con.createStatement();
        String createTemp = "CREATE TEMPORARY TABLE temp_table (oid int);";
        stTemp.execute(createTemp);
        for (int i = 1; i <= maxOid; i++) {
            Statement insertTemp = con.createStatement();
            String insert = "insert into temp_table values(" + i + ");";
            st.execute(insert);
        }


        // Fill Table
        for (String attributeName : attributeNames) {
            String oidQuery = "select oid from h2v where key = '" + attributeName + "';";
            String valQuery = "select val from h2v where key = '" + attributeName + "';";

            Statement stOid = con.createStatement();
            Statement stVal = con.createStatement();

            ResultSet rs1 = stOid.executeQuery(oidQuery);
            ResultSet rs2 = stVal.executeQuery(valQuery);

            while (rs1.next() && rs2.next()) {
                String oidString = rs1.getString(1);
                int oid = Integer.parseInt(oidString);
                String val = rs2.getString(1);
                String insertValues = "insert into v2h (oid, " + attributeName + ") values(" + oid + ", '" + val + "');";
                System.out.println(insertValues);
                st.executeUpdate(insertValues);
            }
        }



        /*
        Statement stOid = con.createStatement();
        String selectOids = "Select distinct oid from v2h order by oid;";
        ResultSet rsOids = stOid.executeQuery(selectOids);
        while (rsOids.next()) {
            System.out.println(rsOids.getString(1));
        }   */



    }



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
                //System.out.println(sql);
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

    public static void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
        if (num_attributes <= 0 || num_tuples <= 0 || num_attributes > 1600 || sparsity > 1.0 || sparsity < 0.0) {
            throw new SQLException("generate() args wrong!!!");
        }
        Statement st = con.createStatement();
        String sql = "DROP Table if exists H;";
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
                    sb.append("a" + i + " VARCHAR(20)");
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
        StringBuilder insert = new StringBuilder("INSERT INTO H VALUES ");
        int oid = 1;
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
                            attributeValue = generateRandomString(1, 5);
                        } else {
                            attributeValue = Integer.toString(RANDOM.nextInt(num_tuples));
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




}