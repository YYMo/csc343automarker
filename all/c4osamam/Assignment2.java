import java.sql.*;

/**
 * Embedded SQL Queries for Assignment 2.
 * @author Muhammad Osama - 1000513018 - c4osamam
 * @note Apologies for the messy code, it isn't well written Java.
 */
public class Assignment2 {

	// A connection to the database  
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	//Class name of the SQL driver
	private String driver = "org.postgresql.Driver";

	//CONSTRUCTOR
	Assignment2() throws ClassNotFoundException{
		Class.forName(driver);
	}

	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
	public boolean connectDB(String URL, String username, String password){
		try{
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException e){
			return false;
		}

		if (connection != null)
			return true;

		this.disconnectDB();
		return false;
	}

	//Closes the connection. Returns true if closure was successful
	public boolean disconnectDB(){
		try{
			connection.close();
		} catch (SQLException e){
			return false;
		}
		return true;    
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		boolean result = false;

		try {
			sql = connection.createStatement();

			String sqlText = "INSERT INTO a2.country VALUES (" +
					cid + ", '" + name + "', " + height + ", " + population + ")";

			result = sql.executeUpdate(sqlText) == 1;
			sql.close();

		} catch (SQLException e) {
			return false;
		}

		return result;
	}

	public int getCountriesNextToOceanCount(int oid) {
		int result = -1;

		try {
			sql = connection.createStatement();

			String sqlText  = "SELECT count(cid) AS count FROM a2.oceanAccess WHERE oid = " + oid;
			rs = sql.executeQuery(sqlText);

			if (rs.next())
				result = rs.getInt("count");

			sql.close();
			rs.close();

		} catch (SQLException e) {
			return -1;
		}

		return result;
	}

	public String getOceanInfo(int oid){
		String result = "";

		try{
			sql = connection.createStatement();

			String sqlText = "SELECT * FROM a2.ocean WHERE oid = " + oid;
			rs = sql.executeQuery(sqlText);

			if (rs.next())
				result = rs.getInt("oid") + ":" +  rs.getString("oname") + ":" + rs.getInt("depth");

			sql.close();
			rs.close();

		} catch (SQLException e){
			return "";
		}

		return result;
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		boolean result;

		try {
			sql = connection.createStatement();

			String sqlText = "UPDATE a2.hdi SET hdi_score=" +  newHDI + " WHERE cid=" + cid + " AND year=" + year;
			result = sql.executeUpdate(sqlText) == 1;

			sql.close();

		} catch (SQLException e) {
			return false;
		}
		return result;
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		boolean result;

		try {
			sql = connection.createStatement();

			String sqlText = "DELETE FROM a2.neighbour WHERE (country=" + c1id + " AND neighbor=" + c2id
					+ ") OR (country=" + c2id + " AND neighbor=" + c1id + ")";
			result = sql.executeUpdate(sqlText) == 2;

			sql.close();
		} catch (SQLException e) {
			return false;
		}
		return result;
	}

	public String listCountryLanguages(int cid){
		String result = "", sqlText;
		double population;

		try {
			sql = connection.createStatement();

			sqlText = "SELECT population FROM a2.country WHERE cid=" + cid;
			rs = sql.executeQuery(sqlText);
			
			if (!rs.next())
				return result;
			
			population = rs.getInt("population");
			rs.close();

			sqlText = "SELECT lid, lname, lpercentage" +
					" FROM a2.language WHERE cid=" + cid;
			rs = sql.executeQuery(sqlText);
			while (rs.next()){
				result += rs.getInt("lid") + ":"
						+ rs.getString("lname") + ":"
						+ rs.getFloat("lpercentage")*population + "#";
			}

			result = result.substring(0, result.length()-1);

			rs.close();
			sql.close();
		} catch (SQLException e) {
			return "";
		}

		return result;
	}

	public boolean updateHeight(int cid, int decrH){
		int ch;
		boolean result = false;

		try {
			sql = connection.createStatement();

			String sqlText = "SELECT height FROM a2.country WHERE cid=" + cid;
			rs = sql.executeQuery(sqlText);

			if (!rs.next())
				return result;
			
			ch = rs.getInt("height") - decrH;

			sqlText = "UPDATE a2.country SET height=" + ch + " WHERE cid=" + cid;
			result = sql.executeUpdate(sqlText) == 1;

			sql.close();
			rs.close();
		} catch (SQLException e) {
			return false;
		}
		return result;
	}

	public boolean updateDB(){
		try {
			sql = connection.createStatement();

			String sqlText = "CREATE TABLE a2.mostPopulousCountries(cid int, cname varchar(20))";
			sql.executeUpdate(sqlText);

			sqlText = "INSERT INTO a2.mostPopulousCountries (" + 
					" SELECT cid, cname" +
					" FROM a2.country" +
					" WHERE population>100000000" +
					" GROUP BY cid, cname" + 
					" ORDER BY cid ASC)";
			sql.executeUpdate(sqlText);

			sql.close();
		} catch (SQLException e) {
			return false;
		}

		return true;    
	}

}
