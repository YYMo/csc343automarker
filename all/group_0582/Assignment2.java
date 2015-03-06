import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	// Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// e.printStackTrace();
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
		return true;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			ps.close();
			rs.close();
			connection.close();
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			ps = connection
					.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?);");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			if (ps.executeUpdate() == 0)
				return false;
			return true;
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			ps = connection
					.prepareStatement("SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = ?;");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			// e.printStackTrace();
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		try {
			ps = connection
					.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?;");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			rs.next();
			// If there's no row for next() to go to, an SQLexception will be
			// thrown upon calling getSting()
			return rs.getString(1) + ":" + rs.getString(2) + ":"
					+ rs.getString(3);
		} catch (SQLException e) {
			// e.printStackTrace();
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			ps = connection
					.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?;");
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			if (ps.executeUpdate() == 0)
				return false;
			return true;
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			ps = connection
					.prepareStatement("DELETE FROM a2.neighbour " +
							"WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?);");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			if (ps.executeUpdate() == 0)
				return false;
			return true;
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
	}

	public String listCountryLanguages(int cid) {
		int population;
		String out = "";
		try {
			ps = connection
					.prepareStatement("SELECT population FROM a2.country WHERE cid = ?;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			rs.next();
			population = rs.getInt(1);
			ps = connection
					.prepareStatement("SELECT * FROM a2.language WHERE cid = ?;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			while (rs.next()) {
				out += rs.getString(2) + ":" + rs.getString(3) + ":"
						+ (rs.getDouble(4) * population) + "#";
			}
			return out.substring(0, out.length() - 1);
		} catch (SQLException e) {
			// e.printStackTrace();
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		int oldHeight;
		try {
			ps = connection
					.prepareStatement("SELECT height FROM a2.country WHERE cid = ?;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			rs.next();
			oldHeight = rs.getInt(1);
			ps = connection
					.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?;");
			ps.setInt(1, oldHeight - decrH);
			ps.setInt(2, cid);
			if (ps.executeUpdate() == 0)
				return false;
			return true;
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
	}

	public boolean updateDB() {
		try {
			ps = connection
					.prepareStatement("CREATE TABLE a2.mostPopulousCountries " +
							"(cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL);");
			ps.executeUpdate();
		} catch (SQLException e) {
			// e.printStackTrace();
		}
		try {
			ps = connection.prepareStatement("DELETE FROM a2.mostPopulousCountries;");
			ps.executeUpdate();
			ps = connection
					.prepareStatement("INSERT INTO a2.mostPopulousCountries " +
							"(SELECT cid, cname FROM a2.country " +
							"WHERE population > 100000000 ORDER BY cid ASC);");
			if (ps.executeUpdate() == 0) return false;
			return true;
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
	}

}


