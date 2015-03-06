import java.sql.*;
import java.util.Calendar;

/**
 * If you are interested, you can download the submitted Main.java
 * to test this assignment.
 * 
 * @author Amir Sobhi (c4sobhia)
 */
public class Assignment2 {

	// #########################################################################
	// Original stub
	// #########################################################################

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
	}

	/***
	 * Using the input parameters, establish a connection to be used for this
	 * session. Returns true if connection is successful
	 * 
	 * @param URL
	 * @param username
	 * @param password
	 * @return
	 */
	public boolean connectDB(String URL, String username, String password) {
		if (!resolveDriver()) return false;
		try {
			// turn URL into proper jdbc connection string
			/*
			String prefix = String.format(CONNECTION_STRING_FORMAT, "");
			if (!URL.startsWith(prefix))
				URL = String.format(CONNECTION_STRING_FORMAT, URL);
			
			if (connection == null || connection.isClosed()) {
				connection = DriverManager.getConnection(URL, 
						username, password);
				createStatement().execute(
						String.format("SET search_path to '%s'", 
						SCHEMA_NAME));
*/
			connection = DriverManager.getConnection(URL, 
						username, password);
			}
			return connection != null && !connection.isClosed();
		} catch (Exception err) {
			log(err, true);
			return false;
		}
	}

	/**
	 * Closes the connection. Returns true if closure was successful
	 * 
	 * @return
	 */
	public boolean disconnectDB() {
		if (connection != null)
			try {
				closeStatements();
				connection.close();
			} catch (Exception e) {
				log(e, true);
				return false;
			}
		return true;
	}

	/**
	 * 
	 * @param cid
	 * @param name
	 * @param height
	 * @param population
	 * @return true iff row was inserted
	 */
	public boolean insertCountry(int cid, String name, int height,
			int population) {
		try {
			ps = createPeparedStatement(
				"insert into country " + 
				" (cid, cname, height, population) values (?,?,?,?)");
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			return ps.executeUpdate() > 0;
		} catch (Exception err) {
			log(err, true);
			return false;
		}
	}

	/**
	 * Returns the number of countries in table 'oceanAccess' 
	 * that are located next to the ocean with  id oid. 
	 * Returns -1 if an error occurs.
	 * @param oid
	 */
	public int getCountriesNextToOceanCount(int oid) {
		try {
			ps = createPeparedStatement(
				"select count(distinct(cid)) from oceanaccess where oid = ?");
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs != null)
				rs.first();
			return rs.getInt(1);
		} catch (Exception err) {
			log(err, true);
		}
		return -1;
	}

	
	/***
	 * Returns a string with the information of an ocean with id oid.
	 * @param oid  the ocean id
	 * @return The output is "oid:oname:depth". 
	 * Returns an empty string "" if the ocean does not exist.
	 */
	public String getOceanInfo(int oid) {
		try {
			ps = createPeparedStatement(
					"select oid, oname, depth from ocean where oid = ?");
			ps.setInt(1, oid);
			return getContiguous(ps.executeQuery(), 3);
		} catch (Exception e) {
			log(e, true);
		}
		return "";
	}
	

	/**
	 * Changes the HDI value of the country cid for 
	 * the year year to the HDI value supplied (newHDI).
	 * @param cid  The country id of country 
	 * @param year The year whose hdi score we want change
	 * @param the new HDI score (a value between 0 and 1)
	 * @return 
	 * true if the change was successful, false otherwise.
	 */
	public boolean chgHDI(int cid, int year, float newHDI) {
		Calendar c = Calendar.getInstance();
		if (newHDI<0 || newHDI > 1 || 
				year < c.getActualMinimum(Calendar.YEAR) ||
				year > c.getActualMaximum(Calendar.YEAR)) return false;
		try {
			ps = createPeparedStatement(
					"update hdi set hdi_score = ? where cid = ? and year = ?");
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setInt(3, year);
			return ps.executeUpdate() > 0;
		} catch (Exception err) {
			log(err, true);
		}
		return false;
	}

	/**
	 * Deletes the neighboring relation between two countries.
	 * @param c1id cid of first country
	 * @param c2id cid of the other country
	 * @return 
	 * true if the deletion was successful, false otherwise. 
	 */
	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			ps = createPeparedStatement("delete from neighbour where " +
					"(neighbor =  ? and country = ? ) or "+ 
					"(country = ? and neighbor = ?)");
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.setInt(3, c1id);
			ps.setInt(4, c2id);
			return ps.executeUpdate() > 0;
		} 
		catch (Exception err)
		{
			log(err, true);
		}
		return false;
	}
	
	/**
	 * Returns a string with all the languages that are spoken in 
	 * the country with id cid. 
	 * 
	 * @param cid the cid of the country
	 * @return 
	 * "l1id:l1lname:l1population#l2id:l2lname:l2population#... "
	 * 		where:
	 * 			lid is the id of the language.
	 * 			lname is name of the country.
	 * 			population is the number of people in a country 
	 * 				that speak the language, note that you will 
	 * 				need to compute this number, as it is not 
	 * 				readily available in the database.
	 * 
	 * Returns an empty string "" if the country does not exist.
	 */
	public String listCountryLanguages(int cid) {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("select x.lid, x.lname, ");
			sb.append("CAST(round(x.pop) as integer) from ( select ");
			sb.append(" l.lid, l.lname, l.lpercentage * c.population as pop ");
			sb.append(" from language l inner join country c on l.cid = c.cid");
			sb.append(" and c.cid = ? ");
			sb.append(") x order by x.pop");
			ps = createPeparedStatement(sb.toString());
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			return getContiguous(rs, 3);
		} catch (Exception err) {
			log(err, true);
		}
		return "";
	}

	/**
	 * Decreases the height of the country with id cid.
	 * @param cid the country to decrease the height for
	 * @param decrH  the absolute amount of decrease
	 * @return true if the record was updated
	 * @see see the piazza thread 326, we assume that decrH < Height 
	 * and doesn't cause the height to become negative.
	 */
	public boolean updateHeight(int cid, int decrH) {
		if (decrH <= 0) return false; // as per @piazza 326
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("update country set height = case when ");
			sb.append(" height - ? < 0 then height else height - ? end ");
			sb.append(" where cid = ? ");
			ps = createPeparedStatement(sb.toString());
			ps.setInt(1, decrH);
			ps.setInt(2, decrH);
			ps.setInt(3, cid);
			return ps.executeUpdate() > 0;
		} catch (Exception err) {
			log(err, false);
		}
		return false;
	}

	/**
	 * Create a table containing all the countries which have a 
	 * population over 100 million.
	 * @return
	 */
	public boolean updateDB() {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("CREATE TABLE IF NOT EXISTS mostPopulousCountries(");
			sb.append("cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT,");
			sb.append("cname VARCHAR(20), PRIMARY KEY(cid))");
			ps = connection.prepareStatement(sb.toString());
			ps.executeUpdate();
			createStatement().execute("delete from mostPopulousCountries");
			return createStatement().executeUpdate(
					"insert into mostPopulousCountries (select cid, cname "+
					"from country where population > 100000000 "+ 
							"order by population asc)") > 0;
		} catch (Exception err) {
			log(err, true);
		}
		return false;
	}

	// #########################################################################
	// stuff added by student
	// #########################################################################
	
	
	// -------------------------------------------------------------------------
	// begin: private variables
	// -------------------------------------------------------------------------

	// singleton driver initialization tracker
	private static boolean isDriverInitialized; 
	
	// -------------------------------------------------------------------------
	// end: private variables
	// -------------------------------------------------------------------------


	
	
	// -------------------------------------------------------------------------
	// begin: public variables (let's you configure this object if need be)
	// -------------------------------------------------------------------------
	
	static String DRIVER_NAME = "org.postgresql.Driver";
	static String CONNECTION_STRING_FORMAT = 
			"jdbc:postgresql://%s";
	static boolean LOGGING_ENABLED;
	static String SCHEMA_NAME = "a2";
	
	// -------------------------------------------------------------------------
	// end: public variables
	// -------------------------------------------------------------------------
	

	
	// -------------------------------------------------------------------------
	// begin: private methods
	// -------------------------------------------------------------------------
	
	private static boolean resolveDriver() {
		if (isDriverInitialized) return true;
		try {
			Class.forName(DRIVER_NAME);
			return (isDriverInitialized = true);
		} catch (Exception err) {
			log(err, true);
			return false;
		}
	}
	
	// this method is for me (the student) to debug using my Main.java file 
	private static void log(Object obj, boolean error) {
		if (!LOGGING_ENABLED) return;
		if (error) System.err.println(obj);
		else System.out.println(obj);
	}
	
	// tries to turn a ResultSet into a contiguous result format
	// specified in assignment specs
	private String getContiguous(ResultSet result, int columnCount) 
			throws SQLException {
		StringBuilder sb = new StringBuilder();
		if (result != null) {
			while (result.next()) {
				for (int c = 1; c <= columnCount; c++)
					sb.append(String.format("%s%s",
							result.getObject(c), c < columnCount ? ":" : ""));
				sb.append("#");
			}
		}
		String str = sb.toString();
		return str.isEmpty() ? str : str.substring(0,str.length()-1);
	}
	
	// ensures one statement is run at a time by closing previous ones
	private void closeStatements() {
		try {
			if (sql != null && !sql.isClosed())
				sql.close();
			if (ps != null && !ps.isClosed())
				ps.close();
		} catch (Exception err) {
			log(err, true);
		}
	}
	
	// -------------------------------------------------------------------------
	// end: private methods
	// -------------------------------------------------------------------------
	

	
	// -------------------------------------------------------------------------
	// begin: package/public methods (used in Main.java and in this class)
	// -------------------------------------------------------------------------
	
	// returns a scroll-able statement
	Statement createStatement() {
		try {
			if (connection == null || connection.isClosed())
				return null;
			closeStatements();
			return connection.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
		} catch (Exception err) {
			log(err, true);
			return null;
		}
	}
	
	// returns a scroll-able preped statement
	PreparedStatement createPeparedStatement(String sql) {
		try {
			if (connection == null || connection.isClosed())
				return null;
			
			closeStatements();
			return connection.prepareStatement(sql,
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
		} catch (Exception err) {
			log(err, true);
			return null;
		}
	}
	
	// -------------------------------------------------------------------------
	// end: public methods
	// -------------------------------------------------------------------------
	
}
