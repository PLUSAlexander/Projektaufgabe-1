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
        generate(25, 0.1, 6);
    }

    public static void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
        Statement st = con.createStatement();
        String sql = "DROP Table if exists H;";
        st.execute(sql);

        String[] attributs = new String[num_attributes];
        for (int i = 0; i < num_attributes; i++) {
            attributs[i] = (i % 2 == 0)? "String" : "int";
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE H (");
        for (int i = 0; i < num_attributes; i++) {
            if (attributs[i].equals("String")) {
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

        StringBuilder insert = new StringBuilder("INSERT INTO H VALUES ");
        for (int i = 0; i < num_tuples; i++) {
            insert.append("(");
            for (int j = 0; j < num_attributes; j++) {
                if (RANDOM.nextDouble() <= sparsity) {
                    insert.append("NULL");
                } else {
                    if (attributs[j].equals("String")) {
                        insert.append("'" + generateRandomString(1, 20) + "'");
                    } else {
                        insert.append(RANDOM.nextInt(100));
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
