import java.sql.*;
import java.io.*;

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
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
		}
		try {
			connection = DriverManager.getConnection(URL, username, password);
			// System.out.println("connected");
			return true;
		} catch (SQLException se) {

		}
		return false;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
				rs.close();
				ps.close();
				connection.close();
			// System.out.println("closed");
			return true;
		} catch (SQLException se) {

		}
		return false;
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		// if connected
		// check if country already exists in table
		String query;
		query = "SELECT " + String.valueOf(cid) + " FROM A2.country";
		try {
			// System.out.println("looking for id");
			ps = connection.prepareStatement(query);
		} catch (SQLException se) {
			return false;
		}

		try {
			if (!(ps.executeQuery()).next()) {
				// System.out.println("inserting entry");
				query = "INSERT INTO A2.country VALUES (" + String.valueOf(cid)
						+ ", '" + name + "' , " + String.valueOf(height) + ", "
						+ String.valueOf(population) + ")";
				try {
					ps = connection.prepareStatement(query);
					ps.executeUpdate();
				} catch (SQLException se) {

					return false;
				}
			} else {
				// System.out.println("id " + cid + "Already exists");
				return false;
			}

		} catch (SQLException se) {

		}

		return false;
	}

	public int getCountriesNextToOceanCount(int oid) {
		String query = "SELECT COUNT(oid) AS num FROM A2.oceanAccess WHERE oid = "
				+ String.valueOf(oid);
		int numofcountries = -1;
		try {
			// System.out.println("Searching for countries");
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				// System.out.println("counting");
				numofcountries = rs.getInt("num");
				// System.out.println(numofcountries);
			}
		} catch (SQLException se) {

			return -1;
		}

		return numofcountries;
	}

	public String getOceanInfo(int oid) {
		String query = "SELECT * FROM A2.ocean WHERE oid = "
				+ String.valueOf(oid);
		String result = "";
		String soid = String.valueOf(oid);
		String oname = "";
		String depth = "";
		try {
			// System.out.println("Searching for OCEAN info");
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				// System.out.println("counting");
				oname = rs.getString("oname");
				depth = String.valueOf(rs.getInt("depth"));
				result = soid + ":" + oname + ":" + depth;
				// System.out.println(numofcountries);
			}
		} catch (SQLException se) {

		}

		return result;
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		String query = "SELECT * from A2.hdi where year ="
				+ String.valueOf(year) + " and cid =" + String.valueOf(cid);
		try {
			// System.out.println("Searching for hdi");
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			// if entry for such year exists
			if (!rs.next()) {
				// System.out.println("No such entry found!");
				return false;
			} else {
				query = "UPDATE A2.hdi SET hdi_score =" + newHDI
						+ "WHERE cid = " + cid + "and year =" + year;

				try {
					// System.out.println("Updating newHDI");
					ps = connection.prepareStatement(query);
					ps.executeUpdate();
					return true;
				} catch (SQLException se) {

					return false;
				}
			}

		} catch (SQLException se) {

			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		String query_cid = "select * from A2.neighbour where country = " + c1id
				+ " and neighbor =" + c2id;
		try {
			ps = connection.prepareStatement(query_cid);
			rs = ps.executeQuery();
			if (!rs.next()) {
				// System.out.println("cid not found in entry");
				return false;

			}
		} catch (SQLException se) {

		}
		String query1 = "DELETE FROM A2.neighbour where country =" + c1id
				+ "and neighbor =" + c2id;
		String query2 = "DELETE FROM A2.neighbour where country =" + c2id
				+ "and neighbor =" + c1id;

		try {
			// System.out.println("Searching for neighbor");
			ps = connection.prepareStatement(query1);
			ps.executeUpdate();

		} catch (SQLException se) {
			return false;
		}

		try {
			// System.out.println("Searching for neighbor opposite");
			ps = connection.prepareStatement(query2);
			ps.executeUpdate();
			return true;
		} catch (SQLException se) {
			return false;
		}

	}

	public String listCountryLanguages(int cid) {
		String result = "";
		String lid = "";
		String lname = "";
		String pop = "";
		String query = "select lid, lname, (lpercentage/100*population) as"
				+ " ppopulation from A2.language, A2.country where A2.language.cid = country.cid and language.cid ="
				+ cid + " order by ppopulation";
		try {
			// System.out.println("Searching for languages");
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			while (rs.next()) {
				lid = String.valueOf(rs.getInt("lid"));
				lname = rs.getString("lname");
				pop = String.valueOf(rs.getInt("ppopulation"));
				result = result + lid + lname + pop + "#";
			}
		} catch (SQLException se) {
			return "";
		}

		return result;
	}

	public boolean updateHeight(int cid, int decrH) {
		String searchQ = "select * from A2.country where cid = " + cid;
		String query = "update A2.country SET height = (height -" + decrH
				+ ") where cid =" + cid;
		try {
			ps = connection.prepareStatement(searchQ);
			rs = ps.executeQuery();
			if (!rs.next()) {
				// System.out.println("cid not found in entry");
				return false;

			}
		} catch (SQLException se) {
		}

		try {
			ps = connection.prepareStatement(query);
			ps.executeUpdate();
			return true;

		} catch (SQLException se) {
		}

		return false;
	}

	public boolean updateDB() {
		String query1 = "DROP TABLE A2.mostPopulousCountries CASCADE";
		String query2 = "CREATE TABLE A2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20)	NOT NULL, PRIMARY KEY(cid))";
		String query3 = "SELECT cid, cname FROM A2.country WHERE population > 100000000 ORDER BY cid ASC";
		String queryInsert = "";
		try {
			// drop table
			// System.out.println("dropping table");
			ps = connection
					.prepareStatement("DROP TABLE A2.mostPopulousCountries CASCADE");
			ps.executeUpdate();

		} catch (SQLException se) {
		}
		try {
			// System.out.println("creating table...");
			ps = connection.prepareStatement(query2);
			ps.executeUpdate();

		} catch (SQLException se) {
		}
		try {
			// System.out.println("Q3");
			ps = connection.prepareStatement(query3);
			rs = ps.executeQuery();
			// System.out.println("selecting....>100million");
			while (rs.next()) {
				try {
					// System.out.println("inserting");
					queryInsert = "INSERT INTO A2.mostPopulousCountries VALUES("
							+ String.valueOf(rs.getInt("cid"))
							+ ",'"
							+ rs.getString("cname") + "')";
					ps = connection.prepareStatement(queryInsert);
					rs = ps.executeQuery();
					// System.out.println("inserted");
					return true;
				} catch (SQLException se) {
				}

			}
			// System.out.println("done ...");
		}

		catch (SQLException se) {
		}
		return false;
	}
}
