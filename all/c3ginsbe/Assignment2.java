import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	Statement sql;

	// The text to give to sql to run queries
	String sqlText;
	
	// Prepared Statement
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException E) {
			System.out.println("PSQL driver not found.");
		}
	}
	
	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			//Create a statement object for later use
			sql = connection.createStatement();
		} catch (SQLException E) {
			return false;
		}
		return connection != null;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try{
			connection.close();
			return true;
		}
		catch (SQLException E){
			return false;
		}
	}
	
	public boolean insertCountry(int cid, String name, int height,
			int population) {

		try{
			//run the query
			sqlText = String.format("INSERT INTO a2.country VALUES (%d,'%s',%d,%d)", cid, name, height, population);			
			
			//check that it was successful
			return sql.executeUpdate(sqlText) == 1;
		}
		catch(SQLException E){
			return false;
		}
	}
	
	public int getCountriesNextToOceanCount(int oid) {

		try{
			sqlText = String.format("SELECT count(cid) FROM a2.oceanAccess WHERE oid = %d", oid);		
			
			rs = sql.executeQuery(sqlText);
			rs.next();
			int result = rs.getInt(1);
			rs.close();
						
			return result;
		}
		catch (SQLException E){
			return -1;
		}
	}

	public String getOceanInfo(int oid) {
		try{
			sqlText = String.format("SELECT * FROM a2.ocean WHERE oid = %d", oid);
			rs = sql.executeQuery(sqlText);
			rs.next();
			String result = String.format("%d:%s:%d", oid, rs.getString(2), rs.getInt(3));
			rs.close();
			return result;
		}
		catch(SQLException E){
			return "";
		}
	}
	
	public boolean chgHDI(int cid, int year, float newHDI) {
		try{
			//change the table
			sqlText = String.format("UPDATE a2.hdi SET hdi_score = %f WHERE cid = %d AND year = %d", newHDI, cid, year);

			return sql.executeUpdate(sqlText) == 1;
		}
		catch (SQLException E){
			return false;
		}
	}

	public boolean deleteNeighbour(int c1id, int c2id) {
		try{
			
			int result = 0;
			//change the table
			sqlText = String.format("DELETE FROM a2.neighbour WHERE country = %d AND neighbor = %d", c1id, c2id);
			result += sql.executeUpdate(sqlText);
			sqlText = String.format("DELETE FROM a2.neighbour WHERE neighbor = %d AND country = %d", c1id, c2id);
			result += sql.executeUpdate(sqlText);

			//check the # of rows is correct
			return result == 2;
		}
		catch (SQLException E){
			return false;
		}
	}

	public String listCountryLanguages(int cid) {
				
		try{
			//do the query
			sqlText = String.format(
					"SELECT L.lid, C.cname, C.population * L.lpercentage " +
					"FROM a2.country C, a2.language L " +
					"WHERE C.cid = %d AND C.cid = L.cid " +
					"ORDER BY C.population * L.lpercentage", cid);
			rs = sql.executeQuery(sqlText);
			StringBuffer result = new StringBuffer();
			
			//build the result string
			while (rs.next()){
				result.append(String.format("%d:%s:%d", rs.getInt(1), rs.getString(2), rs.getInt(3)));
				result.append("#");
			}
			rs.close();
			if (result.length() > 0){
				result.deleteCharAt(result.lastIndexOf("#"));				
			}

			return result.toString();
		}
		catch (SQLException E){
			return"";
		}
	}

	public boolean updateHeight(int cid, int decrH) {
		try{
			sqlText = String.format("UPDATE a2.country SET height = height - %d WHERE cid = %d", decrH, cid);
			return sql.executeUpdate(sqlText) == 1;
		}
		catch (SQLException E){
			return false;
		}
	}

	public boolean updateDB() {
		try{
			//create the new table
			sqlText = "CREATE TABLE a2.mostPopulousCountries(" +
					"cid INTEGER," +
					"cname VARCHAR(20)" +
					")";
			sql.executeUpdate(sqlText);
			
			//insert in the values into the new table
			sqlText = "INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC)";
			sql.executeUpdate(sqlText);
			
			return true;
		}
		catch(SQLException E){
			return false;
		}
	}
	
}
