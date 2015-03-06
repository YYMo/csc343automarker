import java.sql.*;
import java.util.ArrayList;

public class Assignment2 {

	// A connection to the database  
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	// DAVID: I WILL NOT USE PREPARED STATS
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	//CONSTRUCTOR
	Assignment2() throws ClassNotFoundException{
		Class.forName("org.postgresql.Driver");
	}

	// --- GETTERS AND SETTERS --- //

	public Connection getConnection() {
		return this.connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public Statement getSql() {
		return this.sql;
	}

	public void setSql(Statement sql) {
		this.sql = sql;
	}

	public PreparedStatement getPreparedStatement() {
		return this.ps;
	}

	public void setPreparedStatement(PreparedStatement ps) {
		this.ps = ps;
	}

	public ResultSet getResultSet() {
		return this.rs;
	}

	public void setResultSet(ResultSet rs) {
		this.rs = rs;
	}

	// --- END GETTERS AND SETTERS --- //

	/**
	 * Using the String input parameters which are the URL, username, and
	 * password respectively, establish the Connection to be used for this session.
	 * Returns true if the connection was successful.
	 */
	public boolean connectDB(String URL, String username, String password) {

		// If there is an error caught, the method will return false
		try {
			this.connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Closes the connection.
	 * 
	 * Returns true if the closure was successful. If not, return false.
	 */
	public boolean disconnectDB() {

		try {

			// close the ResultSet if it exists
			if (this.rs != null) {
				this.rs.close();
			}

			// close the Statement if it exists
			if (this.sql != null) {
				this.sql.close();
			}

			// close the Connection if it exists
			if (this.connection != null) {
				this.connection.close();
			}

			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Checks whether country with id 'cid' already exists in the database. If it does not, insert a row into the country table.
	 * 
	 * If the insertion was successful, return true. If not, return false.
	 * 
	 * cid is the id of the country, name is the name of the country, height is the highest
	 * elevation point and population is the population of the newly inserted country.
	 * 
	 */
	public boolean insertCountry (int cid, String name, int height, int population) {

		@SuppressWarnings("unused")
		int result;

		try {
			this.setSql(this.getConnection().createStatement());

			// Check whether the country with id 'cid' already exists in the database
			this.rs = this.getSql().executeQuery("SELECT cid FROM a2.country WHERE cid='" +
					cid + "';");

			// if there is no next value, the ResultSet is empty
			if (!this.rs.next()) {
				// execute the update to the database
				result = this.getSql().executeUpdate("INSERT INTO a2.country VALUES ('" + cid + "', '" + name + "', '" + height +
						"', '" + population + "');");
				// case where the value did not exist and was added
				return true;
			}
			// case where the value already existed in the DB
			return false;
		} catch (SQLException e) {
			// Note: If I get here, I have no idea what the data looks like
			// case where an exception was thrown/something went wrong
			return false;
		}
	}

	/**
	 * Returns the number of countries in table oceanAccess that are
	 * located next to the ocean with id oid.
	 * 
	 * Return -1 if an error occurs.
	 */
	public int getCountriesNextToOceanCount(int oid) {

		try {

			this.setSql(this.getConnection().createStatement());

			// check whether oid exists
			this.rs = this.getSql().executeQuery("SELECT oid FROM a2.ocean WHERE "
					+ "oid='" + oid + "';");
			// if the result set is empty, meaning that the ocean does not exist
			if (!this.rs.next()) {
				return -1;
			} else {
				// the case where the ocean exists
				this.rs = this.getSql().executeQuery("SELECT oid FROM a2.oceanaccess WHERE "
						+ "oid='" + oid + "';");

				int count = 0;

				// count the number of countries in the result set
				while (this.rs.next()) {
					count = count + 1;
				}

				return count;
			}
		} catch (SQLException e) {
			return -1;
		}
	}

	/**
	 * Returns a string with the information of an ocean with id oid.
	 * The output is oid:oname:depth.
	 * 
	 * Return an empty string if the ocean does not exist.
	 */
	public String getOceanInfo(int oid){

		try {
			this.setSql(this.getConnection().createStatement());

			// check whether oid exists
			this.rs = this.getSql().executeQuery("SELECT * FROM a2.ocean WHERE "
					+ "oid='" + oid + "';");

			// if the result set is empty, meaning that the ocean does not exist
			if (!this.rs.next()) {
				return "";
			} else {
				// the case where the ocean with id 'oid' exists

				String ID = rs.getString("oid");
				String oname = rs.getString("oname");
				String depth = rs.getString("depth");

				// return the formatted String
				return "" + ID + ":" + oname + ":" + depth;
			}
		} catch (SQLException e) {
			return "-1";
		}
	}

	/**
	 * Changes the HDI value of the country cid for the year year to the HDI
	 * value supplied (newHDI). Return true if the change was successful,
	 * false otherwise.
	 */
	public boolean chgHDI(int cid, int year, float newHDI){

		try {
			this.sql = sql.getConnection().createStatement();

			// check whether a record with id 'cid' and year 'year' exists
			this.rs = this.sql.executeQuery("SELECT * FROM a2.hdi WHERE cid='" +
					cid + "' AND year='" + year + "';");

			// if the record does not exist
			if (!this.rs.next()) {
				return false;
			} else {

				// update the hdi value for the record matching 
				this.sql.executeUpdate("UPDATE a2.hdi SET hdi_score = '" +
						Float.toString(newHDI) + "' WHERE cid = '" + cid + "' AND year = '" + 
						year + "';");

				return true;
			}
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Deletes the neighboring relation between two countries.
	 * Returns true if the deletion was successful, false otherwise.
	 * 
	 * You can assume that the neighboring relation to be deleted exists
	 * in the database. Remember that if c2 is a neighbor of c1, c1 is also
	 * a neighbour of c2.
	 */
	public boolean deleteNeighbour(int c1id, int c2id){

		// technically, this is not required because the parameters passed in are
		// supposed to be valid
		if (c1id == c2id) {
			return false;
		}

		try {
			this.sql = sql.getConnection().createStatement();

			// I will not check whether c1id or c2id exists because the assignment
			// mentions "You can assume that the neighboring relation to be deleted
			// exists in the database", meaning that the parameters passed are valid.

			// remove the country=c1id, neighbor=c2id relationship
			this.sql.executeUpdate("DELETE FROM a2.neighbour WHERE country = '" + 
					c1id + "' AND neighbor = '" + c2id + "';");

			// remove the country=c2id, neighbor=c1id relationship
			this.sql.executeUpdate("DELETE FROM a2.neighbour WHERE country = '" + 
					c2id + "' AND neighbor = '" + c1id + "';");

			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Returns a string with all the languages that are spoken in the
	 * country with id cid. The list of languages should follow the
	 * contiguous format described above, and contain the following
	 * attributes in the order shown: (NOTE: before creating the string
	 * order your results by population).
	 * 
	 * l1id:l1lname:l1population#l2id:l2lname:l2population#...  where:
	 * - lid is the id of the language.
	 * - lname is name of the language.
	 * - population is the number of people in a country that speak the language, note that you will need to compute this number, as it is not readily available in the database.
	 * 
	 * Returns an empty string if the country does not exist.
	 * 
	 */
	public String listCountryLanguages(int cid){

		try {
			String cattedString = "";
			boolean isFirstRun = true;
			this.sql = sql.getConnection().createStatement();

			// check whether the country exists
			this.rs = this.sql.executeQuery("SELECT * FROM a2.language WHERE cid = '" + 
					cid + "';");

			// if the country does not exist
			if (!this.rs.next()) {
				return "";
			} else {
				// reset the pointer to be able to loop through from the first index
				this.rs = this.sql.executeQuery("SELECT * FROM a2.language WHERE cid = '" +
						cid + "' ORDER BY lpercentage;");

				while (this.rs.next()) {
					// get all the info from the current record
					int langID = rs.getInt("lid");
					String langName = rs.getString("lname");
					float langPerc = rs.getFloat("lpercentage");

					// if first run, we do not have a "#" at the beginning
					if (isFirstRun) {
						// append the info to the concatenated string
						cattedString = cattedString + Integer.toString(langID) + ":" +
								langName + ":" + langPerc;
						isFirstRun = false;
					} else {
						cattedString = cattedString + "#" + Integer.toString(langID) + ":" +
								langName + ":" + langPerc;
					}
				}
				return cattedString;
			}
		} catch (SQLException e) {
			// return -1 if there is an error
			return "-1";
		}
	}

	/**
	 * Decreases the height of the country with id cid. (A decrease might
	 * happen due to natural erosion.)
	 * 
	 * Return true if the update was successful, false otherwise.
	 */
	public boolean updateHeight(int cid, int decrH){

		try {
			this.sql = sql.getConnection().createStatement();

			// check whether the country exists
			this.rs = this.sql.executeQuery("SELECT * FROM a2.country WHERE cid = '" +
					cid + "';");

			// if the country does not exist
			if (!this.rs.next()) {
				return false;
			} else {

				// get the height from the table and calculate the new height
				int height = Integer.parseInt(rs.getString("height"));
				int newHeight = height - decrH;

				// update the table with the new height
				this.sql.executeUpdate("UPDATE a2.country SET height = '" + newHeight +
						"' WHERE cid = '" + cid + "';");

				// check whether the change was made correctly
				this.rs = this.sql.executeQuery("SELECT * FROM a2.country WHERE cid = '" +
						cid + "';");

				this.rs.next();
				String ID = rs.getString("cid");
				String updatedHeight = rs.getString("height");

				if (ID.equals(Integer.toString(cid)) &&
						updatedHeight.equals(Integer.toString(newHeight))) {
					return true;
				} else {
					return false;
				}
			}
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Create a table containing all the countries which have a population
	 * over 100 million. The name of the table should be
	 * mostPopulousCountries and the attributes should be:
	 * - cid INTEGER (country id)
	 * - cname VARCHAR(20) (country name)
	 * 
	 * Return true if the database was successfully updated, false otherwise.
	 * 
	 * Store the results in ASC order according to the country id (cid).
	 */
	public boolean updateDB(){

		// find all the countries that have population > 100
		try {
			@SuppressWarnings("unused")
			int result;

			// create a list to store the values of our result set
			ArrayList<String> cidList = new ArrayList<String>();
			ArrayList<String> cnameList = new ArrayList<String>();

			this.sql = sql.getConnection().createStatement();

			this.rs = this.sql.executeQuery("SELECT cid, cname FROM a2.country WHERE " + 
					"population > 100 ORDER BY cid ASC;");

			// parse the resultSet

			// if there are no countries with pop > 100
			if (!this.rs.next()) {
				// need to return an empty table

				// drop old table if it exists
				result = this.sql.executeUpdate("DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE;");

				// create the table and attributes
				result = this.sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries (" +
						"cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL, primary key(cid));");

				// this should return true because there was no error and there
				// were simply no results for that query
				return true;
			} else {
				// if there are countries with pop > 100

				this.rs = this.sql.executeQuery("SELECT cid, cname FROM a2.country WHERE " + 
						"population > 100 ORDER BY cid ASC;");

				while (this.rs.next()) {

					String ID = rs.getString("cid");
					String cname = rs.getString("cname");

					cidList.add(ID);
					cnameList.add(cname);
				}

				// drop old table if it exists
				result = this.sql.executeUpdate("DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE;");

				// create the table and attributes
				result = this.sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries (" +
						"cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL, primary key(cid));");

				// Insert records into the new table from the parsed data
				for (int i = 0; i < cidList.size(); i++) {
					result = this.sql.executeUpdate("INSERT INTO a2.mostpopulouscountries VALUES ('" + 
							cidList.get(i) + "', '" + cnameList.get(i) + "');");
				}
				return true;
			}

		} catch (SQLException e) {
			return false;
		}
	}
}
