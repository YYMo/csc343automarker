import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// Result set for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is successful
	public boolean connectDB(String URL, String username, String password) {

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			return false;
		}

		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}
	}

	// Closes the connection. Returns true if closure was successful
	public boolean disconnectDB() {
		try {
			if (connection != null) {
				connection.close();
			}
			return true;
		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}
	}

	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			sql = connection.createStatement();
			String sql_query = "INSERT INTO a2.country VALUES " + "(" + cid
					+ "," + "'" + name + "'" + "," + height + "," + population
					+ ");";
			sql.executeUpdate(sql_query);
			return true;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		try {
			int counter = 0;
			sql = connection.createStatement();

			String sql_query = "SELECT cid FROM a2.oceanAccess WHERE oid="
					+ oid + ";";
			ResultSet rs = sql.executeQuery(sql_query);

			while (rs.next()) {
				counter += 1;
			}
			rs.close();

			return counter;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		String oceanInfo = "";
		try {
			sql = connection.createStatement();
			String sql_query = "SELECT * FROM a2.ocean WHERE oid=" + oid + ";";
			ResultSet rs = sql.executeQuery(sql_query);

			while (rs.next()) {
				// Retrieve by column name
				int temp_oid = rs.getInt("oid");
				String oname = rs.getString("oname");
				int depth = rs.getInt("depth");

				oceanInfo = temp_oid + ":" + oname + ":" + depth;
			}
			rs.close();
			return oceanInfo;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			sql = connection.createStatement();

			String sql_query = "UPDATE a2.hdi " + "SET hdi_score = " + newHDI
					+ " WHERE cid=" + cid + " AND year=" + year + ";";

			int count = sql.executeUpdate(sql_query);
			if (count == 0) {
				return false;
			}
			return true;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}

	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			sql = connection.createStatement();

			String sql_query = "DELETE FROM a2.neighbour " + "WHERE country="
					+ c1id + " AND neighbor=" + c2id + ";";
			int count = sql.executeUpdate(sql_query);

			if (count == 0) {
				return false;
			}

			sql_query = "DELETE FROM a2.neighbour " + " WHERE country=" + c2id
					+ " AND neighbor=" + c1id + ";";
			count = sql.executeUpdate(sql_query);

			if (count == 0) {
				return false;
			}

			return true;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}

	}

	public String listCountryLanguages(int cid) {
		try {
			sql = connection.createStatement();

			String sql_query = "SELECT language.lname, language.lid, language.lpercentage, country.population FROM "
					+ "a2.language join a2.country on language.cid=country.cid WHERE country.cid="
					+ cid + ";";
			ResultSet rs = sql.executeQuery(sql_query);

			String listLanguges = "";
			while (rs.next()) {
				// Retrieve by column name
				int lid = rs.getInt("lid");
				String lname = rs.getString("lname");
				double popPercent = rs.getDouble("lpercentage");
				int countryPopulation = rs.getInt("population");
				double languagePopulation = countryPopulation * popPercent;
				// Display values
				listLanguges += "|" + lid + ":|" + lname + ":|"
						+ languagePopulation + "#";
			}
			rs.close();

			return listLanguges;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		
		if (decrH < 0) {
			return false;
		}
		
		try {
			sql = connection.createStatement();
			int height = 0;

			String sql_query = "SELECT height FROM a2.country WHERE cid=" + cid
					+ ";";
			ResultSet rs = sql.executeQuery(sql_query);
			while (rs.next()) {
				height = rs.getInt("height");
			}
			rs.close();

			sql_query = "UPDATE a2.country " + "SET height = "
					+ (height - decrH) + " WHERE cid=" + cid + ";";

			int count = sql.executeUpdate(sql_query);
			if (count == 0) {
				return false;
			}
			return true;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}

	}

	public boolean updateDB() {
		try {
			sql = connection.createStatement();

			String tableName = "mostPopulousCountries";

			// Drop table if it exists
			String sql_query = "DROP TABLE IF EXISTS " + "a2." + tableName
					+ " CASCADE;";
			sql.executeUpdate(sql_query);

			// Create the table
			sql_query = "CREATE TABLE "
					+ "a2."
					+ tableName
					+ " (cid INTEGER not NULL REFERENCES country(cid) ON DELETE RESTRICT, "
					+ " cname VARCHAR(20) not NULL);";
			sql.executeUpdate(sql_query);

			// Find all country with population over a million
			String sql_select_countries = "SELECT * FROM a2.country WHERE population > 1000000 ORDER BY cid ASC;";
			ResultSet rs = sql.executeQuery(sql_select_countries);

			Statement temp_sql = connection.createStatement();
			// Insert each country into the new table
			while (rs.next()) {
				int cid = rs.getInt("cid");
				String cname = rs.getString("cname");

				String insert_query = "INSERT INTO " + "a2." + tableName + " VALUES ("
						+ cid + ",'" + cname + "');";
				temp_sql.executeUpdate(insert_query);
			}
			rs.close();

			return true;

		} catch (SQLException se) {
			// Handle errors for JDBC
			return false;
		}

	}

}
