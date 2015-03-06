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
			return;
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			String realURL = "jdbc:postgresql://" + URL;
			connection = DriverManager.getConnection(realURL, username,
					password);
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
		try {
			String query1 = "select cid from a2.country where cid = ?";
			ps = connection.prepareStatement(query1);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs.next()) {
				return false;
			}
			String query2 = "insert into a2.country values (?, ? , ?, ?)";
			ps = connection.prepareStatement(query2);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			String query = "select count(cid) as CoNextToOid from a2.oceanAccess where oid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt("CoNextToOid");
			} else {
				return -1;
			}
		} catch (SQLException e) {
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		String answer = "";
		try {
			String query = "select * from a2.ocean where oid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()) {
				int actualOid = rs.getInt("oid");
				String Oname = rs.getString("oname");
				int depth = rs.getInt("depth");
				answer = answer + String.valueOf(actualOid) + ":" + Oname + ":"
						+ String.valueOf(depth);
			}
			return answer;
		} catch (SQLException e) {
			return answer;
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			String query = "select cid, year from a2.hdi where cid = ? and year = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			ps.setInt(2, year);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return false;
			}
			query = "update a2.hdi set hdi_score = ? where cid = ? "
					+ "and year = ?";
			ps = connection.prepareStatement(query);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			ps.executeUpdate();
			return true;
		} catch (SQLException e) {
			return false;
		}

	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			String query = "delete from a2.neighbour where ((country = ? and "
					+ "neighbor = ?) or (country = ? and neighbor = ?))";
			ps = connection.prepareStatement(query);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c2id);
			ps.setInt(4, c1id);
			ps.executeUpdate();
			return true;
		}

		catch (SQLException e) {
			return false;
		}

	}

	public String listCountryLanguages(int cid) {
		String Answer = "";
		try {
			String query = "select lid, lname, lpercentage * population as numofpeople from a2.language, "
					+ "a2.country where language.cid = country.cid and language.cid = ? order by population";
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			while (rs.next()) {
				int currentlid = rs.getInt("lid");
				String currentlname = rs.getString("lname");
				int currentnum = rs.getInt("numofpeople");
				Answer = Answer + String.valueOf(currentlid) + ":"
						+ currentlname + ":" + String.valueOf(currentnum) + "#";
			}
			return Answer;
		} catch (SQLException e) {
			return Answer;
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		try {
			String query = "select cid, height from a2.country where cid = ?";
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs.next()) {
				int newheight = rs.getInt("height") - decrH;
				String query1 = "update a2.country set height = ? where cid = ?";
				ps = connection.prepareStatement(query1);
				ps.setInt(1, newheight);
				ps.setInt(2, cid);
				ps.executeUpdate();
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean updateDB() {
		try {
			String Query = "CREATE TABLE a2.mostPopulousCountries as (select cid, cname from a2.country where population > 100000000 order by cid ASC)";
			ps = connection.prepareStatement(Query);
			ps.executeUpdate();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

}
