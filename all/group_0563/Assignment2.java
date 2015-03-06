import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Assignment2 {

	/**
	 * The name of the table created by {@link #updateDB()}.
	 */
	private static final String POP_TABLE_NAME = "mostPopulousCountries";

	/**
	 * A connection to the database.
	 */
	private Connection connection;

	/**
	 * Constructor. Makes an attempt to load the JDBC driver.
	 */
	public Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {}
	}

	/**
	 * Using the input parameters, establish a connection to be used for this session.
	 * @return true if connection is sucessful, false otherwise.
	 */
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			if (connection == null) {
				return false;
			}
		} catch (SQLException e) {
			return false;
		}

		try {
			connection.prepareStatement("SET search_path TO a2").execute();
			return true;
		} catch (SQLException e) {
			disconnectDB();
			return false;
		}
	}

	/**
	 * Attempts to close the connection.
	 * @return true if closure was sucessful, false otherwise.
	 */
	public boolean disconnectDB() {
		try {
			connection.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Inserts a row into the country table.
	 * @param cid the id of the country; must not already exist in the table
	 * @param name the name of the country
	 * @param height the highest elevation point
	 * @param population the country's population
	 * @return true if successul, false otherwise.
	 */
	public boolean insertCountry(int cid, String name, int height, int population) {
		try (PreparedStatement ps = connection.prepareStatement(
				"INSERT INTO country VALUES (?, ?, ?, ?)")) {
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			return ps.executeUpdate() == 1;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Returns the number of countries in table "oceanAccess"
	 * that are located next to ocean with id oid.
	 * @param oid
	 * @return the number of countries in the table, or -1 if an error occurs.
	 */
	public int getCountriesNextToOceanCount(int oid) {
		try (PreparedStatement ps = connection.prepareStatement(
				"SELECT COUNT(*) FROM oceanAccess WHERE oid=?")) {
			ps.setInt(1, oid);
			ResultSet rs = ps.executeQuery();
			return !rs.next() ? -1 : rs.getInt(1);
		} catch (SQLException e) {
			return -1;
		}
	}

	/**
	 * Returns a string with the information of an ocean with id oid.
	 * @param oid
	 * @return "oid:oname:depth", or an empty string if the ocean does not exist.
	 */
	public String getOceanInfo(int oid) {
		try (PreparedStatement ps = connection.prepareStatement(
				"SELECT * FROM ocean WHERE oid=?")) {

			ps.setInt(1, oid);
			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				return "";
			}

			StringBuilder sb = new StringBuilder();
			sb.append(rs.getInt("oid"));
			sb.append(':');
			sb.append(rs.getString("oname"));
			sb.append(':');
			sb.append(rs.getInt("depth"));

			return sb.toString();
		} catch (SQLException e) {
			return "";
		}
	}

	/**
	 * Changes the HDI value of the country cid for the given year to the HDI value supplied.
	 * @param cid
	 * @param year
	 * @param newHDI
	 * @return true if successul, false otherwise.
	 */
	public boolean chgHDI(int cid, int year, float newHDI) {
		try (PreparedStatement ps = connection.prepareStatement(
				"UPDATE hdi SET hdi_score=? WHERE cid=? AND year=?")) {
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			return ps.executeUpdate() == 1;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Deletes the neighboring relation between two countries.
	 * Assumed that the neighboring relation to be deleted exists in the database.
	 * (Remember that if c2 is a neighbor of c1, c1 is also a neighbour of c2.)
	 * @param c1id
	 * @param c2id
	 * @return true if successul, false otherwise.
	 */
	public boolean deleteNeighbour(int c1id, int c2id) {
		try (PreparedStatement ps = connection.prepareStatement(
				"DELETE FROM neighbour WHERE country=? AND neighbor=?")) {
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			if (ps.executeUpdate() != 1) {
				return false;
			}

			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			return ps.executeUpdate() == 1;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Returns a string with all the languages that are spoken in the country with id cid,
	 * with the list ordered by population.
	 * @param cid the id of the country to list language information of
	 * @return "l1id:l1lname:l1population#l2id:l2lname:l2population#..."
	 * <p>where:
	 * <br>-lid is the id of the language
	 * <br>-lname is the name of the country
	 * <br>-population is the number of people in a country that speak the language,
	 * note that this number needs to be computed, as it is not readily available in the database.
	 * <p>Returns an empty string if the country does not exist.
	 */
	public String listCountryLanguages(int cid) {
		// Assumes 0<=percentage<=1, as stated on Piazza @329.
		try (PreparedStatement ps = connection.prepareStatement(
				"SELECT lid, lname, (lpercentage * population) AS lpopulation "
				+ "FROM language NATURAL JOIN country "
				+ "WHERE cid = ? "
				+ "ORDER BY lpopulation")) {

			ps.setInt(1, cid);
			StringBuilder sb = new StringBuilder();
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				sb.append(rs.getInt("lid"));
				sb.append(':');
				sb.append(rs.getString("lname"));
				sb.append(':');
				sb.append(rs.getInt("lpopulation"));
				sb.append('#');
			}
			sb.setLength(Math.max(sb.length()-1, 0)); // Delete final '#'
			return sb.toString(); // Will be empty if the cid wasn't found; rs will have no rows
		} catch (SQLException e) {
			return "";
		}
	}

	/**
	 * Decreases the height of the country with id cid.
	 * @param cid the country to decrease the height of.
	 * @param decrH how much to decrease the height by.
	 * @return true if successful, false otherwise.
	 */
	public boolean updateHeight(int cid, int decrH) {
		try (PreparedStatement ps = connection.prepareStatement(
				"UPDATE country SET height=height-? WHERE cid=?")){
			ps.setInt(1, decrH);
			ps.setInt(2, cid);
			return ps.executeUpdate() == 1;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	 * Create a table containing all the countries which have a population over 100
	 * million. The name of the table should be mostPopulousCountries and the
	 * attributes should be:
	 * <p>-cid INTEGER (country id)
	 * <br>-cname VARCHAR(20) (country name)
	 * <p>Store the results in ASC order according to the country id (cid).
	 * @return true if the database was updated, false otherwise.
	 */
	public boolean updateDB() {
		try {
			connection.prepareStatement("DROP TABLE IF EXISTS " + POP_TABLE_NAME).executeUpdate();
			connection.prepareStatement(
					"CREATE TABLE " + POP_TABLE_NAME + " ("
							+ "cid INTEGER REFERENCES country(cid),"
							+ "cname VARCHAR(20))").executeUpdate();

			connection.prepareStatement(
					"INSERT INTO " + POP_TABLE_NAME + " ("
					+ "SELECT cid, cname "
					+ "FROM country "
					+ "WHERE population > 100000000 "
					+ "ORDER BY cid ASC)").executeUpdate();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

}
