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
			System.out.println("Failed to find the JDBC driver");
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username,
					password);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			ps.close();
			rs.close();
			sql.close();
			connection.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			ps = connection
					.prepareStatement("INSERT INTO a2.country (cid, cname, height, population) VALUES (?,?,?,?);");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			ps = connection
					.prepareStatement("SELECT COUNT(*) FROM a2.oceanaccess WHERE oid = ?;");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			rs.next();
			int ans = rs.getInt(1);
			return ans;
		} catch (Exception e) {
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
			int o = rs.getInt(1);
			String s = rs.getString(2);
			int d = rs.getInt(3);
			return (Integer.toString(o) + ":" + s + ":" + Integer.toString(d));
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			ps = connection
					.prepareStatement("UPDATE a2.hdi SET cid=?, year=?, hdi_score=? WHERE cid=? AND year=?;");
			ps.setInt(1, cid);
			ps.setInt(2, year);
			ps.setFloat(3, newHDI);
			ps.setInt(4, cid);
			ps.setInt(5, year);
			ps.executeUpdate();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			ps = connection
					.prepareStatement("DELETE FROM a2.neighbour WHERE country =? AND neighbor = ?;");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.executeUpdate();
			ps = connection
					.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?;");
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			ps.executeUpdate();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public String listCountryLanguages(int cid) {
		try {
			ps = connection
					.prepareStatement("SELECT l.lid, l.lname, c.population * l.lpercentage pop FROM a2.country c, a2.language l WHERE c.cid = l.cid AND c.cid = ? ORDER BY pop;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			String ans = "";
			while (rs.next()) {
				ans = ans + Integer.toString(rs.getInt(1)) + ":"
						+ rs.getString(2) + ":"
						+ Float.toString(rs.getFloat(3)) + "#";
			}
			ans = ans.substring(0, ans.length() - 1);
			return ans;
		} catch (Exception e) {
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			ps = connection
					.prepareStatement("SELECT height FROM a2.country WHERE cid = ?;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			rs.next();
			int h = rs.getInt(1);
			ps = connection
					.prepareStatement("UPDATE a2.country SET height=? WHERE cid = ?;");
			ps.setInt(1, (h - decrH));
			ps.setInt(2, cid);
			ps.executeUpdate();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean updateDB() {
		try {
			ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries (cid Integer, cname VARCHAR(20));");
			ps.executeUpdate();
			ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100e6 ORDER BY cid ASC);");
			ps.executeUpdate();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
