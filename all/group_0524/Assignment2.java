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
			Class.forName("org.postpresql.Driver");
		} catch(ClassNotFoundException e) {
			// class not found
		}
		
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			if (connection == null) {
				return false;
			}

		} catch (Exception e) {
			return false;
		}
		return true;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			if(ps != null ) {
				ps.close();
				if(!ps.isClosed()) {
					return false;
				}
			}
			if(rs != null) {
				rs.close();
				if (!rs.isClosed()) {
					return false;
				}
			}
			if(sql != null) {
				sql.close();
				if (!sql.isClosed()) {
					return false;
				}
			}
			if(connection != null ) {
				connection.close();
				if (!connection.isClosed()) {
					return false;
				}
			}
			
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			ps = connection.prepareStatement("INSERT INTO a2.country "
					+ "VALUES (?,?,?,?)");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			if (ps.executeUpdate() == 1) {
				ps.close();
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return false;
	}

	// Select count(*) from oceanAccess where oid = oid
	public int getCountriesNextToOceanCount(int oid) {
		int count = -1;
		try {
			sql = connection.createStatement();
			rs = sql.executeQuery("SELECT count(*) from a2.oceanAccess WHERE oid ="+oid);
			if(rs == null) {
				sql.close();
				return -1;
			}
			rs.next();
			count = rs.getInt("count");
			rs.close();
			sql.close();
		} catch (SQLException e) {
			return -1;
		}
		return count;
	}

	// select * from ocean where oid = oid
	public String getOceanInfo(int oid) {
		String ans = "";
		try {
			sql = connection.createStatement();
			rs = sql.executeQuery("SELECT * FROM a2.ocean WHERE oid =" + oid);
			if (rs != null) {
				while (rs.next()) {
					ans += rs.getInt("oid") + ":";
					ans += rs.getString("oname") + ":";
					ans += rs.getInt("depth") + "";
				}
			}
			rs.close();
			sql.close();
		} catch (SQLException e) {
			return "";
		}
		return ans;
	}

	// UPDATE hdi SET hdi_score = newHDI WHERE cid = cid, year = year
	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			sql = connection.createStatement();
			sql.executeUpdate("UPDATE a2.hdi " + "SET hdi_score = '" + newHDI
					+ "'" + "WHERE cid = " + cid + "and year = " + year);
			if (sql.getUpdateCount() == 1) {
				sql.close();
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return false;
	}


	// DELETE FROM neighbour WHERE country = c1id and neighbor = c2id
	// DELETE FROM neighbour WHERE country = c2id and neighbor = c1id
	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			sql = connection.createStatement();
			sql.executeUpdate("DELETE FROM a2.neighbour where country=" + c1id
					+ "AND neighbor=" + c2id);
			int updateCount = sql.getUpdateCount();
			sql.executeUpdate("DELETE FROM a2.neighbour where country=" + c2id
					+ "AND neighbor=" + c1id);
			if ((updateCount+sql.getUpdateCount()) == 2) {
				sql.close();
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return false;
	}

	// SELECT language.lid, language.lname,
	// language.lpercentage*country.population
	// FROM language, country WHERE language.cid = cid and country.cid=cid
	public String listCountryLanguages(int cid) {
		String ans = "";
		try {
			sql = connection.createStatement();
			rs = sql.executeQuery("SELECT lid, lname, (lpercentage*population) as Population "
					+ "FROM a2.language, a2.country "
					+ "WHERE country.cid = " + cid);
			if (rs != null) {
				rs.next();
				ans += rs.getInt(1) + ":";
				ans += rs.getString(2) + ":";
				ans += rs.getDouble(3) + "";
				while (rs!=null && rs.next()) {
					ans += "#";
					ans += rs.getInt(1) + ":";
					ans += rs.getString(2) + ":";
					ans += rs.getDouble(3) + "";
				}
				rs.close();
				sql.close();
			}
		} catch (SQLException e) {
			return "";
		}
		
		return ans;
	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			sql = connection.createStatement();
			rs = sql.executeQuery("SELECT height FROM a2.country WHERE cid = "
					+ cid);
			int height = -1;
			if (rs != null && rs.next()) {
				height = rs.getInt("height");
				height -= decrH;
			}
			if (height < 0) {
				return false;
			}
			sql.executeUpdate("UPDATE a2.country SET height = " + height
					+ "WHERE cid = " + cid);
			if (sql.getUpdateCount() == 1) {
				rs.close();
				sql.close();
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return false;
	}

	// select cid, cname FROM country WHERE population > 100 000 000
	public boolean updateDB() {
		try {
			sql = connection.createStatement();
			sql.executeUpdate("DROP TABLE IF EXISTS a2.mostPopulousCountries");

			sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries( "
					+ "cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT, "
					+ "cname VARCHAR(20) NOT NULL " + ")");

			ps = connection
					.prepareStatement("INSERT INTO a2.mostPopulousCountries "
							+ "SELECT cid, cname FROM a2.country "
							+ "WHERE population > 100000000 " + "ORDER BY cid ASC");
			ps.executeUpdate();
			ps.close();
			sql.close();
		} catch (SQLException e) {
			return false;
		}
		return true;
	}
}
