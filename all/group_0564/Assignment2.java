import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Assignment2 {

    // A connection to the database
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // ResultSet for the query
    ResultSet rs;

    Assignment2() {
        try {
            // Load JDBC driver
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
        }
    }

    // Using the input parameters, establish a connection to be used for this session.
    // Returns true if connection is successful
    public boolean connectDB(String URL, String username, String password) {
        try {
            connection = DriverManager.getConnection("jdbc:postgresql://" + URL, username, password);
            return connection != null;
        } catch (SQLException e) {
		e.printStackTrace();
            return false;
        }
    }

    // Closes the connection. Returns true if closure was successful
    public boolean disconnectDB() {
        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean insertCountry(int cid, String name, int height, int population) {
        try {
            ps = connection.prepareStatement(
                    "INSERT INTO a2.country VALUES (?, ?, ?, ?)"
            );
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);

            int retVal = ps.executeUpdate();
            ps.close();

            return retVal > 0;
        } catch (SQLException e) {
 	    //e.printStackTrace();
            return false;
        }
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            ps = connection.prepareStatement(
                    "SELECT COUNT(*) " +
                    "FROM a2.oceanAccess " +
                    "WHERE oid = ?"
            );
            ps.setInt(1, oid);
            rs = ps.executeQuery();

            int retVal = -1;
            if (rs.next()) {
                retVal = rs.getInt(1);
            }

            rs.close();
            ps.close();

            return retVal;
        } catch (SQLException e) {
            return -1;
        }
    }

    public String getOceanInfo(int oid) {
        try {
            ps = connection.prepareStatement(
                    "SELECT oid, oname, depth " +
                    "FROM a2.ocean " +
                    "WHERE oid = ? "
            );
            ps.setInt(1, oid);
            rs = ps.executeQuery();

            String retVal = "";
            if (rs.next()) {
                int retOid = rs.getInt(1);
                String retName = rs.getString(2);
                int retDepth = rs.getInt(3);

                retVal = retOid + ":" + retName + ":" + retDepth;
            }

            rs.close();
            ps.close();

            return retVal;
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        try {
            ps = connection.prepareStatement(
                    "UPDATE a2.hdi " +
                    "SET hdi_score = ? " +
                    "WHERE cid = ? AND year = ?"
            );
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);

            int retVal = ps.executeUpdate();
            ps.close();

            return retVal > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        try {
            ps = connection.prepareStatement(
                    "DELETE FROM a2.neighbour " +
                    "WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?)"
            );
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);

            int retVal = ps.executeUpdate();
            ps.close();

            return retVal > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    public String listCountryLanguages(int cid) {
        try {
            ps = connection.prepareStatement(
                    "SELECT lid, lname, population * lpercentage " +
                    "FROM a2.country, a2.language " +
                    "WHERE a2.country.cid = ? AND a2.language.cid = a2.country.cid " +
                    "ORDER BY lpercentage"
            );
            ps.setInt(1, cid);
            rs = ps.executeQuery();

            String retVal = "";
            while (rs.next()) {
                int retLid = rs.getInt(1);
                String retName = rs.getString(2);
                int retPop = rs.getInt(3);

                retVal += retLid + ":" + retName + ":" + retPop + "#";
            }
            if (!retVal.isEmpty()) {
                retVal = retVal.substring(0, retVal.length() - 1); // remove trailing '#'
            }

            rs.close();
            ps.close();

            return retVal;
        } catch (SQLException e) {
            return "";
        }
    }

    // The method decreases the original height by decrH (the new value should
    // be: height-decrH). You can assume that decrH < height, so the new value
    // will always be greater than or equal to 0.
    public boolean updateHeight(int cid, int decrH) {
        try {
            ps = connection.prepareStatement(
                    "UPDATE a2.country " +
                    "SET height = ((SELECT height FROM a2.country WHERE cid = ?) - ?) " +
                    "WHERE cid = ?"
            );
            ps.setInt(1, cid);
            ps.setInt(2, decrH);
            ps.setInt(3, cid);

            int retVal = ps.executeUpdate();
            ps.close();

            return retVal > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean updateDB() {
        try {
            String create =
                    "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries(" +
                        "cid int, " +
                        "cname VARCHAR(20)" +
                    ")";

            sql = connection.createStatement();
            sql.executeUpdate(create);
            sql.close();

            ps = connection.prepareStatement(
                    "INSERT INTO a2.mostPopulousCountries " +
                    "SELECT cid, cname " +
                    "FROM a2.country " +
                    "WHERE population > 100e6 " +
		    "ORDER BY cid ASC"
            );
            ps.executeUpdate();
            ps.close();

            return true;
        } catch (SQLException e) {
	    //e.printStackTrace();
            return false;
        }
    }
}
