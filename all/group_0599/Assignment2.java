import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Assignment2 {
    static String LOCAL_HOST_URL = "jdbc:postgresql://localhost:5432/";

    // A connection to the database
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    // CONSTRUCTOR
    Assignment2() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    // Using the input parameters, establish a connection to be used for this
    // session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            this.connection = DriverManager.getConnection(URL, username,
                    password);
        } catch (Exception e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    // Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public boolean insertCountry(int cid, String name, int height,
            int population) {
        try {
            this.sql = this.connection.createStatement();
            this.rs = this.sql.executeQuery("SELECT * FROM country "
                    + "WHERE cid = " + cid);
            if (this.rs.next()) {
                this.rs.close();
                return false;
            }
            this.sql.executeUpdate("INSERT INTO country VALUES(" + cid + ",\'"
                    + name + "\'," + height + "," + population + ")");
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public int getCountriesNextToOceanCount(int oid) {
        int count;
        try {
            this.sql = this.connection.createStatement();
            this.rs = this.sql.executeQuery("SELECT COUNT(*) FROM "
                    + "oceanAccess WHERE oid = " + oid);
            this.rs.next();
            count = this.rs.getInt(1);
            this.rs.close();
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return -1;
        }
        return count;
    }

    public String getOceanInfo(int oid) {
        String res = "";
        try {
            this.sql = this.connection.createStatement();
            this.rs = this.sql.executeQuery("SELECT * FROM "
                    + "ocean WHERE oid = " + oid);
            while (this.rs.next()) {
                res = this.rs.getInt(1) + ":" + this.rs.getString(2) + ":"
                        + this.rs.getInt(3);
            }
            this.rs.close();
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return "";
        }
        return res;
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        int res = 0;
        try {
            this.sql = this.connection.createStatement();
            res = this.sql.executeUpdate("UPDATE hdi SET hdi_score = " + newHDI
                    + " WHERE cid = " + cid + " AND year = " + year);
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return res == 0 ? false : true; // return false if no HDI was changed
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        try {
            this.sql = this.connection.createStatement();
            this.sql.executeUpdate("DELETE FROM neighbour WHERE country = "
                    + c1id + " AND neighbor = " + c2id);
            this.sql.executeUpdate("DELETE FROM neighbour WHERE country = "
                    + c2id + " AND neighbor = " + c1id);
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public String listCountryLanguages(int cid) {
        StringBuilder sb = new StringBuilder();
        try {
            this.sql = this.connection.createStatement();
            this.rs = this.sql
                    .executeQuery("SELECT lid, lname, (lpercentage * population) AS pop "
                            + "FROM country c, language l "
                            + "WHERE c.cid = l.cid " + "AND l.cid = " + cid
                            + " ORDER BY pop");
            while (this.rs.next()) {
                sb.append(this.rs.getInt(1) + ":");
                sb.append(this.rs.getString(2) + ":");
                sb.append((int) this.rs.getFloat(3) + "#");
            }
            // remove trailing #
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length()-1);
            }
            this.rs.close();
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return "";
        }
        return sb.toString();
    }

    public boolean updateHeight(int cid, int decrH) {
        int newHeight = 0;
        try {
            this.sql = this.connection.createStatement();
            this.rs = this.sql.executeQuery("SELECT height "
                    + "FROM country WHERE cid = " + cid);
            if (!this.rs.next()) {
                return false; // no cid found in table
            }
            newHeight = this.rs.getInt(1) - decrH;
            this.sql.executeUpdate("UPDATE country SET height = " + newHeight
                    + " WHERE cid = " + cid);
            this.sql.close();
        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public boolean updateDB() {
        try {
            String tableName = "mostpopulouscountries";
            DatabaseMetaData meta = this.connection.getMetaData();
            this.sql = this.connection.createStatement();
            this.rs = meta.getTables(null, null, tableName, null);
            if (this.rs.next()) { // drop existing table
                this.rs.close();
                this.sql.execute("DROP TABLE " + tableName);
            }
            this.sql.executeUpdate("CREATE TABLE " + tableName
                    + " AS SELECT cid, cname FROM country "
                    + "WHERE population > 100000000 ORDER BY cid ASC");
            this.sql.close();
        } catch (Exception e) {
            System.out.println(e);
            return false;
        }
        return true;
    }

//    public static void main(String args[]) {
//        if (args.length != 2) {
//            System.out.println("Enter 2 arguments: (assuming no "
//                    + "password is needed for accessing the database)");
//            System.out.println("arg #1: DATABASE_NAME");
//            System.out.println("arg #2: USER_NAME");
//            return;
//        }
//        String db = args[0];
//        String usr = args[1];
//        Assignment2 a2 = new Assignment2();
//        a2.connectDB(LOCAL_HOST_URL + db, usr, "");
//        try {
//            a2.connection.createStatement().executeUpdate(
//                    "DELETE FROM country WHERE cid = 5");
//            System.out.println(a2.insertCountry(5, "Korea", 10, 900));
//            System.out.println(a2.insertCountry(5, "Korea", 10, 900));
//            a2.sql = a2.connection.createStatement();
//            a2.rs = a2.sql.executeQuery("select * from country");
//            while (a2.rs.next()) {
//                System.out.println(a2.rs.getInt(1) + " (" + a2.rs.getString(2)
//                        + ")");
//            }
//            a2.rs.close();
//            a2.sql.close();
//            System.out.println("Number of country near ocean \'2\' is "
//                    + a2.getCountriesNextToOceanCount(2));
//            System.out.println(a2.getOceanInfo(7));
//            System.out.println(a2.getOceanInfo(1));
//            System.out.println(a2.chgHDI(1, 2009, 1));
//            // TODO ask prof if no update is done should return true
//            System.out.println(a2.chgHDI(1, 1, 1));
//            System.out.println(a2.deleteNeighbour(1, 2));
//            System.out.println(a2.listCountryLanguages(1));
//            System.out.println(a2.updateHeight(1, 1)); // Canada
//            System.out.println(a2.updateHeight(100, 1)); // no such cid in table
//            a2.insertCountry(6, "X", 10, 100000002);
//            a2.insertCountry(7, "Y", 10, 100000001);
//            System.out.println(a2.updateDB());
//            a2.connection.createStatement().executeUpdate(
//                    "DELETE FROM country WHERE cid = 6");
//            a2.connection.createStatement().executeUpdate(
//                    "DELETE FROM country WHERE cid = 7");
//        } catch (Exception e) {
//            System.err.println(e);
//        }
//        a2.disconnectDB();
//    }
}
