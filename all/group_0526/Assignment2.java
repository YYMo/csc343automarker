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
	
	// Query String
	String qs;
	
	//CONSTRUCTOR
	Assignment2(){
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("Failed to find the JDBC driver");
		}
	}

	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try {

			// Assuming the URL is localhost:5432/csc343h-<cdf-login>
			String fullURL = "jdbc:postgresql://" + URL;
			connection = DriverManager.getConnection(fullURL, username, password);

			return true;
		} catch (SQLException se) {
			return false;
		}
	}
	
	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try {
			connection.close();
			return true;
		} catch (SQLException se) {
			return false;
		}
	}
	
	public boolean insertCountry (int cid, String name, int height, int population) {
		try {
			sql = connection.createStatement(); 

			// Put single quotes around the country name
			qs = "INSERT INTO a2.country VALUES (" + cid + ", '" + name + "', " +
				height + ", " + population + ")";

			int n = sql.executeUpdate(qs);
			sql.close();

			return n == 1;
		} catch (SQLIntegrityConstraintViolationException se) {
			return false;
		} catch (SQLException se) {
			return false;
		}
	}
	
	public int getCountriesNextToOceanCount(int oid) {
		try {
			int n = 0;

			sql = connection.createStatement(); 

			// Verify an ocean with oid exists
			qs = "SELECT * FROM a2.ocean WHERE oid = " + oid;
			
			rs = sql.executeQuery(qs);

			if (rs != null && !rs.next()) {
				return -1;
			}

			qs = "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = " + oid;
			
			rs = sql.executeQuery(qs);
			
			if (rs != null && rs.next()) {
				n = rs.getInt("count");
			}

			sql.close();
			rs.close();

			return n;
		} catch(SQLException se) {
			return -1;
		}
	}
	
	public String getOceanInfo(int oid){
		try {
			String info = "";

			sql = connection.createStatement(); 
			qs = "SELECT * FROM a2.ocean WHERE oid = " + oid;
			
			rs = sql.executeQuery(qs);
			
			if (rs != null && rs.next()) {
				info = (rs.getInt("oid") + ":" + rs.getString("oname") + ":" +
						rs.getInt("depth"));
			}

			sql.close();
			rs.close();

			return info;
		} catch(SQLException se) {
			return "";
		}
	}
	
	public boolean chgHDI(int cid, int year, float newHDI){
		try {
			sql = connection.createStatement();
			qs = "UPDATE a2.hdi SET hdi_score = " + newHDI + 
				" WHERE hdi.cid = " + cid + " AND hdi.year = " + year;

			int n = sql.executeUpdate(qs);

			sql.close();

			return n == 1;
		} catch (SQLException se) {
			return false;
		}
	}
	
	public boolean deleteNeighbour(int c1id, int c2id){
		try {
			boolean isSuccess = false;
			int n = 0;

			sql = connection.createStatement();
			qs = "DELETE FROM a2.neighbour WHERE country = " + c1id +
				" AND neighbor = " + c2id;
			
			n = sql.executeUpdate(qs);
			isSuccess = n == 1;

			qs = "DELETE FROM a2.neighbour WHERE country = " + c2id + 
				" AND neighbor = " + c1id;
			
			n = sql.executeUpdate(qs);
			isSuccess = isSuccess && n == 1;
			
			sql.close();

			return isSuccess;
		} catch(SQLException se) {
			return false;
		}
	}
	
	public String listCountryLanguages(int cid){
		try {
			String lang = "";

			sql = connection.createStatement();

			// Assuming 0 <= lpercentage <= 1
			qs = "SELECT lid, lname, (lpercentage * population) AS p " +
				"FROM a2.language AS l, a2.country AS c WHERE l.cid = c.cid " +
				"AND l.cid = " + cid + " ORDER BY p";

			rs = sql.executeQuery(qs);
			
			while (rs != null && rs.next()) {
				String row = rs.getInt("lid") + ":" + rs.getString("lname") +
					":" + rs.getFloat("p");

				if (lang.equals("")) {
					lang = lang + row;
				} else {
					lang = lang + "#" + row;
				}
			}

			sql.close();
			rs.close();

			return lang;
		} catch(SQLException se) {
			return "";
		}
	}
	
	public boolean updateHeight(int cid, int decrH){
		try {
			sql = connection.createStatement();
			qs = "UPDATE a2.country SET height = (country.height - " + 
				decrH + ") WHERE country.cid = " + cid;
			
			int n = sql.executeUpdate(qs);
			
			sql.close();
			
			return n == 1;
		} catch (SQLException se) {
			return false;
		}
	}
	
	public boolean updateDB(){
		try {
			int n = 0;
			int m = 0;

			sql = connection.createStatement();

			qs = "SELECT COUNT(*) FROM a2.country WHERE population > 100000000";
			rs = sql.executeQuery(qs);

			if (rs != null && rs.next()) {
				n = rs.getInt("count");
			}

			qs = "CREATE TABLE a2.mostPopulousCountries AS " +
				"SELECT cid, cname " + 
				"FROM a2.country " +
				"WHERE population > 100000000 " +
				"ORDER BY cid ASC";
			
			m = sql.executeUpdate(qs);
			
			sql.close();
			
			return n == m;
		} catch (SQLException se) {
			return false;
		}
	}
}


