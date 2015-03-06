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
			// TODO Check
			// System.err.println("Failed to find the JDBC driver");
			// System.exit(-1);
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}
		return true;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			connection.close();
		} catch (SQLException e) {
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}
		return true;
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		String insert = "INSERT INTO country (cid, cname, height, population) VALUES (?, ?, ?, ?)";
		try {
			ps = connection.prepareStatement(insert);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);

			int ret = ps.executeUpdate();

			if (ret != 1) {
				// System.err.println("Error while inserting!");
				return false;
			}

			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}

		return true;
	}

	public int getCountriesNextToOceanCount(int oid) {
		String query = "SELECT COUNT(cid) FROM oceanAccess WHERE oid = ?";
		int count;
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);

			rs = ps.executeQuery();

			if (!rs.next()) {
				// System.err.println("Error while executing query: " + query);
				return -1;
			}

			count = rs.getInt(1);

			rs.close();
			ps.close();

		} catch (SQLException e) {
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return -1;
		}
		return count;
	}

	public String getOceanInfo(int oid) {
		String query = "SELECT oname, depth FROM ocean WHERE oid = ?";
		String ret = "";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);

			rs = ps.executeQuery();

			if (!rs.next()) {
				return "";
			}

			String oname = rs.getString(1);
			int depth = rs.getInt(2);

			ret = oid + ":" + oname + ":" + depth;

			rs.close();
			ps.close();

		} catch (SQLException e) {
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return "";
		}

		return ret;
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		String update = "UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
		try {
			ps = connection.prepareStatement(update);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);

			int ret = ps.executeUpdate();

			if (ret != 1) {
				// System.err.println("Error while updating hdi!");
				return false;
			}

			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}

		return true;
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		String delete = "DELETE FROM neighbour WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?)";
		try {
			ps = connection.prepareStatement(delete);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);

			int ret = ps.executeUpdate();

			if (ret != 2) {
				// System.err.println("Error while deleting neighbour!");
				return false;
			}

			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}

		return true;
	}

	public String listCountryLanguages(int cid) {
		String query = "SELECT l.lid, l.lname, (l.lpercentage * c.population) AS population "
				+ "FROM language l JOIN country c ON c.cid = l.cid "
				+ "WHERE c.cid = ? " + "ORDER BY population";
		String ret = "";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);

			rs = ps.executeQuery();

			while (rs.next()) {
				int lid = rs.getInt(1);
				String lname = rs.getString(2);
				int population = rs.getInt(3);

				ret += lid + ":" + lname + ":" + population + "#";
			}

			rs.close();
			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return "";
		}

		return ret;
	}

	public boolean updateHeight(int cid, int decrH) {
		String update = "UPDATE country SET height = (height - ?) WHERE cid = ?";
		try {
			ps = connection.prepareStatement(update);
			ps.setInt(1, decrH);
			ps.setInt(2, cid);

			int ret = ps.executeUpdate();

			if (ret != 1) {
				// System.err.println("Error while updating height!");
				return false;
			}

			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
			// System.err.println("SQL Exception." + "<Message>: "
			// + e.getMessage());
			return false;
		}

		return true;
	}

	public boolean updateDB() {
		String create = "CREATE TABLE IF NOT EXISTS mostPopulousCountries("
				+ "cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT, "
				+ "cname VARCHAR(20))";
		String insert = "INSERT INTO mostPopulousCountries("
				+ "SELECT cid, cname FROM country WHERE population > 100e6 ORDER BY cid ASC)";
		try {
			ps = connection.prepareStatement(create);
			ps.executeUpdate();
			ps.close();

			ps = connection.prepareStatement(insert);
			ps.executeUpdate();
			ps.close();

		} catch (SQLException e) {
			// TODO Comment out printStackTrace
//			System.err.println("SQL Exception." + "<Message>: "
//					+ e.getMessage());
			return false;
		}

		return true;
	}

}
