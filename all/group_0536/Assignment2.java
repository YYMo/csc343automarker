/**
* CSC343
* Assignment 2
* @author  Chaoyu Zhang
* @author  Theresa Ma
*/
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
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			sql = connection.createStatement();
			String sqlText = "set search_path to 'A2'";
			sql.executeUpdate(sqlText);
		} catch (SQLException se) {
			return false;
		}

		if (connection != null) {
			return true;
		} else {
			return false;
		}
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			connection.close();
			return true;
		} catch (SQLException se) {
			return false;
		}
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			ps = connection
					.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?)");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			return true;
		} catch (SQLException se) {
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			ps = connection
					.prepareStatement("SELECT COUNT(cid) FROM a2.oceanAccess WHERE oid = ?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return 0;
			} else {
				return rs.getInt("count");
			}
		} catch (SQLException se) {
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		try {
			ps = connection
					.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();

			if (!rs.next()) {
				return "";
			} else {
				String oname = rs.getString("oname");
				int depth = rs.getInt("depth");

				return oid + ":" + oname + ":" + depth;
			}
		} catch (SQLException se) {
			return "";
		}

	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {

			int rows = 0;

			ps = connection
					.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			rows = ps.executeUpdate();

			if (rows == 0) {
				return false;
			}

			else {
				return true;
			}
		} catch (SQLException se) {
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {

		try {
			int rows = 0;

			ps = connection
					.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);

			rows = ps.executeUpdate();

			if (rows == 0) {
				return false;
			} else {
				ps.setInt(1, c2id);
				ps.setInt(2, c1id);

				rows = ps.executeUpdate();
				if (rows == 0) {
					return false;
				}
				return true;
			}
		} catch (SQLException se) {
			return false;
		}
	}

	public String listCountryLanguages(int cid) {
		try {
			ps = connection.prepareStatement("SELECT l.lid, l.lname, "
					+ "(c.population * l.lpercentage) as population "
					+ "FROM a2.country c join a2.language l on c.cid = l.cid "
					+ "WHERE c.cid = ? ORDER BY population");

			ps.setInt(1, cid);
			rs = ps.executeQuery();

			String result = "";

			while (rs.next()) {
				int lid = rs.getInt("lid");
				String lname = rs.getString("lname");
				int population = rs.getInt("population");
				result = result + lid + ":" + lname + ":" + population + "#";
			}
			if (result != "")
				return result.substring(0, result.length() - 1);

			return result;
		}

		catch (SQLException se) {
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			int rows = 0;
			ps = connection.prepareStatement("UPDATE a2.country "
					+ "SET height = height-? WHERE cid = ?");
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			rows = ps.executeUpdate();

			if (rows == 0) {
				return false;
			}

			else {
				return true;
			}
		} catch (SQLException se) {
			return false;
		}
	}

	public boolean updateDB() {
		try {
			sql = connection.createStatement();
			String sqlText;
			sqlText = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries "
					+ "(cid INTEGER PRIMARY KEY, "
					+ " cname VARCHAR(20) NOT NULL)";
			sql.executeUpdate(sqlText);
			sqlText = "DELETE FROM a2.mostPopulousCountries";
			sql.executeUpdate(sqlText);
			sqlText = "INSERT INTO a2.mostPopulousCountries "
					+ "(SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid)";
			sql.executeUpdate(sqlText);
			return true;
		} catch (SQLException se) {
			return false;
		}
	}
}
