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

	// CONSTRUCTOR
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// driver not found
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password)
			throws ClassNotFoundException {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			connection.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		String queryString = "INSERT INTO a2.country(cid, cname, height, population) VALUES (?, ?, ?, ?);";

		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			int result = ps.executeUpdate();
			ps.close();
			if (result == 0){
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		String queryString = "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid=?;";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			while (rs.next()) {
				int result = rs.getInt("count");
				rs.close();
				return result;
			}
			return 0;
		} catch (SQLException e) {
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		String queryString = "SELECT * FROM a2.ocean WHERE oid=?;";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			ps.close();
			while (rs.next()) {
				String result = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
				rs.close();
				return result;
			}
			return "";
		} catch (SQLException e) {
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		String queryString = "UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?;";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			int result = ps.executeUpdate();
			ps.close();
			if (result == 0){
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		String queryString = " DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) OR "
				+ "(country = ? AND neighbor = ?);";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			int result = ps.executeUpdate();
			ps.close();
			if (result == 0){
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	public String listCountryLanguages(int cid) {
		String queryString = "SELECT lid, lname, lpercentage, population FROM a2.language NATURAL JOIN "
				+ "a2.country WHERE cid = ? ORDER BY lpercentage;";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			ps.close();
			String result="";
			while (rs.next()) {
				float numSpeaking = rs.getFloat("lpercentage") * rs.getInt("population");
				result += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + numSpeaking + "#";
			}
			rs.close();
			return result;
		} catch (SQLException e) {
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		String queryString = "UPDATE a2.country SET height = (height - ?) WHERE cid= ?;";
		try {
			ps = connection.prepareStatement(queryString);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			int result = ps.executeUpdate();
			ps.close();
			if (result == 0){
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean updateDB() {
		String queryString = "CREATE TABLE a2.mostPopulousCountries AS "
				+ "(SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC);";
		try {
			ps = connection.prepareStatement(queryString);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}
	

}
