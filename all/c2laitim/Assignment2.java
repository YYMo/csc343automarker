import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// ResultSet for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			return;
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is successful
	public boolean connectDB(String URL, String username, String password) {
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
		// If country.cid already exists do nothing
		boolean alreadyexist = false;
		try {
			ps = connection.prepareStatement("SELECT cid FROM country");
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getInt("cid") == (cid)) {
					// cid already exists
					alreadyexist = true;
					break;
				}
			}

			if (!alreadyexist) {
				// cid does not exist in set
				ps = connection.prepareStatement("INSERT INTO country "
						+ "VALUES (" + cid + ", '" + name + "', " + height
						+ ", " + population + ")");
				ps.executeUpdate();
				rs.close();
				ps.close();
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public int getCountriesNextToOceanCount(int oid) {
		int count = 0;
		try {
			ps = connection.prepareStatement("SELECT oid FROM oceanAccess");
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getInt("oid") == oid) {
					count += 1;
				}
			}
			rs.close();
			ps.close();
			return count;
		} catch (SQLException e) {
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		// "oid:oname:depth"
		try {
			ps = connection.prepareStatement("SELECT * FROM ocean");
			rs = ps.executeQuery();
			while (rs.next()) {
				// Ocean exists
				if (rs.getInt("oid") == oid) {
					String oceanid = rs.getString("oid");
					String oceanname = rs.getString("oname");
					String oceandepth = rs.getString("depth");
					rs.close();
					ps.close();
					return (oceanid + ":" + oceanname + ":" + oceandepth);
					// oid:oname:depth
				}
			}
			// end of set and Ocean does not exist
			rs.close();
			ps.close();
			return "";

		} catch (SQLException e) {
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			String sqlText = "UPDATE hdi SET hdi_score="+newHDI
					+ " WHERE cid = "+cid+" AND year = "+year;
			ps = connection.prepareStatement(sqlText);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			String sqlText1 = "DELETE FROM neighbour WHERE country=" + c1id
					+ " AND neighbor=" + c2id;
			String sqlText2 = "DELETE FROM neighbour WHERE country=" + c2id
					+ " AND neighbor=" + c1id;
			ps = connection.prepareStatement(sqlText1);
			ps.executeUpdate();
			ps = connection.prepareStatement(sqlText2);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public String listCountryLanguages(int cid) {
		String answer = "";
		try {
			String sqlText = "SELECT lid, lname, (population * lpercentage/100) AS population"
					+ " FROM country, language"
					+ " WHERE country.cid = language.cid AND country.cid = "+cid
					+ " ORDER BY population";
			ps = connection.prepareStatement(sqlText);
			rs = ps.executeQuery();
			while (rs.next()) {
				// l1id:l1name:l1population#l2id:l2name:l2population in this
				// form
				String lid = rs.getString("lid");
				String lname = rs.getString("lname");
				String lpopulation = rs.getString("population");
				String currenttuple = lid + ":" + lname + ":" + lpopulation+"#";
				// Append currenttuple to answer
				answer += currenttuple;
			}
			ps.close();
			rs.close();
			return answer;
		} catch (SQLException e) {
			return "";
		}

	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			String sqlText = "UPDATE country SET height=" + decrH
					+ " WHERE cid=" + cid;
			ps = connection.prepareStatement(sqlText);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public boolean updateDB() {
		try {
			// Create the table
			String sqlText1 = "CREATE TABLE mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
			ps = connection.prepareStatement(sqlText1);
			ps.executeUpdate();

			String sqlText2 = "SELECT cid, cname FROM country WHERE population > 100000000";
			ps = connection.prepareStatement(sqlText2);
			rs = ps.executeQuery();
			while (rs.next()) {
				// get cid and cname and insert into mostPopulousCountries table
				String cid = rs.getString("cid");
				String cname = rs.getString("cname");
				String sqlText3 = ("INSERT INTO mostPopulousCountries VALUES("
						+cid+", '"+cname+"')");
				ps = connection.prepareStatement(sqlText3);
				ps.executeUpdate();

			}
			
			rs.close();
			ps.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

}
