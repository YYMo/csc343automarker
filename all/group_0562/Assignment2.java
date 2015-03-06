
import java.sql.*;

public class Assignment2 {

    // A connection to the database  
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    //CONSTRUCTOR
    Assignment2() {
        try {	//Load JDBC driver
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            return;
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            connection = DriverManager.getConnection(URL, username, password);
            if (connection == null) {
                return false;
            }
            sql = connection.createStatement();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean insertCountry(int cid, String name, int height, int population) {
        String stmt = "INSERT INTO a2.country "
                + "VALUES (?, ?, ?, ?);";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setInt(1, cid);
            preStmt.setString(2, name);
            preStmt.setInt(3, height);
            preStmt.setInt(4, population);
            preStmt.executeUpdate();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public int getCountriesNextToOceanCount(int oid) {
        String stmt = "SELECT COUNT(cid) AS num FROM a2.oceanAccess "
                + "WHERE oid=?;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setInt(1, oid);
            ResultSet resSet = preStmt.executeQuery();
            resSet.next();
            return resSet.getInt("num");
	    
        } catch (SQLException e) {
        }
        return -1;
    }

    public String getOceanInfo(int oid) {
        String stmt = "SELECT oid, oname, depth FROM a2.ocean "
                + "WHERE oid=?;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setInt(1, oid);
            ResultSet resSet = preStmt.executeQuery();
            resSet.next();
            String id = Integer.toString(resSet.getInt("oid"));
            String name = resSet.getString("oname");
            String depth = Integer.toString(resSet.getInt("oid"));
            return id+":"+name+":"+depth;
        } catch (SQLException e) {}
        return "";
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        String stmt = "UPDATE hdi SET hdi_score=? WHERE cid=? AND year=?;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setFloat(1, newHDI);
            preStmt.setInt(2, cid);
            preStmt.setInt(3, year);
            preStmt.executeUpdate();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        String stmt = "DELETE FROM a2.neighbour "
                + "WHERE country=? OR neighbor=?;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setInt(1, c1id);
            preStmt.setInt(2, c2id);
            if (preStmt.executeUpdate() == 2) {return true;}
        } catch (SQLException e) {}
        return false;
    }

    public String listCountryLanguages(int cid) {
        String stmt = "SELECT lid, lname, lpercentage*(select population from a2.country where cid=?) as speakers FROM a2.language WHERE cid=? ORDER BY speakers desc;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.setInt(1, cid);
            preStmt.setInt(2, cid);
            ResultSet resSet = preStmt.executeQuery();
	    String total = "";

	    int c = 1;
            while (resSet.next()) {
            	String id = Integer.toString(resSet.getInt("lid"));
            	String name = resSet.getString("lname");
           	String pop = Integer.toString(resSet.getInt("speakers"));
		if ((c != 1)) {
		   total +="#";
		}
		total += id+":"+name+":"+pop;
		c++;
	    }
            return total;
        } catch (SQLException e) {}
        return "";
    }

    public boolean updateHeight(int cid, int decrH) {
         String stmt = "UPDATE a2.country SET height=height-? WHERE cid=?;";
	 try {
		PreparedStatement preStmt = connection.prepareStatement(stmt);
		preStmt.setFloat(1, decrH);
		preStmt.setInt(2, cid);
		preStmt.executeUpdate();
	 } catch (SQLException e) {
	 	return false;
    	 }
	 return true;
    }

    public boolean updateDB() {

        String stmt = "SET search_path TO A2;DROP TABLE IF EXISTS mostPopulousCountries ;" +
                "CREATE TABLE mostPopulousCountries (cid INTEGER, " +
                "cname VARCHAR(20)); INSERT INTO mostPopulousCountries "+
                "SELECT cid, cname FROM country WHERE population>100000000;";
        try {
            PreparedStatement preStmt = connection.prepareStatement(stmt);
            preStmt.executeUpdate();
        } catch (SQLException e) {
            return false;
        }
        return true;
        
    }

}
