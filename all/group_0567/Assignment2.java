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

	//CONSTRUCTOR
	Assignment2(){
	}

	public boolean connectDB(String URL, String username, String password) throws SQLException{
		connection = DriverManager.getConnection(URL, username, password);
		return (connection != null);
	}

	public boolean disconnectDB() throws SQLException{
		connection.close();
		return connection.isClosed();    
	}

	public boolean insertCountry (int cid, String name, int height, int population) {
		String query = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);
			ps.executeUpdate();
			String query2 = "SELECT cid FROM a2.country WHERE cid = ?";

			ps = connection.prepareStatement(query2);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			return rs.next();
		} 
		catch (SQLException e) {
			return false;
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
	}

	public int getCountriesNextToOceanCount(int oid) {
		String query = "SELECT count(cid) AS num FROM a2.oceanAccess GROUP BY oid HAVING oid=?";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			rs.next();
            return rs.getInt(1);
		} 
		catch (SQLException e) {
			return -1;
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		
	}

	public String getOceanInfo(int oid){
		String query = "SELECT * FROM a2.ocean WHERE oid=?";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();
			if (rs.next()){
				int id = rs.getInt(1);
				String oname = rs.getString(2);
				int depth = rs.getInt(3); 
				return id+":"+oname+":"+ depth;
			}
			else {
				return "";
			}
		} 
		catch (SQLException e) {
			return "";
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		   
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
		try {
			ps = connection.prepareStatement(query);
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setFloat(3, year);
			ps.executeQuery();

			String query2 = "SELECT hdi_score FROM a2.hdi WHERE cid = ? AND year = ?";
			ps = connection.prepareStatement(query2);
			rs = ps.executeQuery();
			rs.next();
			return newHDI == rs.getInt("hdi_score");
		} catch (SQLException e) {
			return false;
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
	
	}

	public boolean deleteNeighbour(int c1id, int c2id){
	   String query = "DELETE FROM a2.neighbour WHERE country=? AND neighbor=?";
	   try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.executeUpdate();

			ps = connection.prepareStatement(query);
			ps.setInt(2, c1id);
			ps.setInt(1, c2id);
			ps.executeQuery();

			String querycheck = "SELECT * FROM a2.neighbour WHERE country=? AND neighbor=?";
			ps = connection.prepareStatement(querycheck);
			ps.setInt(1, c1id);
			ps.setInt(2,c2id);
			rs = ps.executeQuery();

			if (rs.next()){
				return false;
			}

			ps = connection.prepareStatement(querycheck);
			ps.setInt(2, c1id);
			ps.setInt(1, c2id);
			rs = ps.executeQuery();

			if (rs.next()){
				return false;
			}

			return true;  
		} catch (SQLException e) {
			return false;
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		         
	}

	public String listCountryLanguages(int cid){
		String query = "SELECT lid, cname, population, lpercentage FROM a2.country" +
				" NATURAL JOIN a2.language WHERE cid=? ORDER BY population";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();

			String result = "";
			while (true){
				if (rs.next()){
					result = result + rs.getInt("lid") + ":";
					result = result + rs.getString("cname") + ":";
					int population = rs.getInt("population");
					float lpercentage = rs.getFloat("lpercentage");
					float ratio = population * lpercentage;
					result = result + ratio + "#";
				}
				else{
					return result;
				}
			}
		} 
		catch (SQLException e) {
			return "";
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		   
	}

	public boolean updateHeight(int cid, int decrH){
		String query = "SELECT HEIGHT FROM a2.country WHERE cid = ?";
		String updateQuery = "UPDATE a2.country SET height = ? WHERE cid = ?";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (!rs.next()){
				return false;
			}
			int height = rs.getInt("height");
			int newHeight = height - decrH;
			ps = connection.prepareStatement(updateQuery);
			ps.setInt(1, newHeight);
			ps.setInt(2, cid);
			ps.executeUpdate();

			String checkQuery = "SELECT height FROM a2.country WHERE cid = ?";
			ps = connection.prepareStatement(checkQuery);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (!rs.next()){
				return false;
			}
			return newHeight == rs.getInt("height");

		} catch (SQLException e) {
			return false;
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		
	}

	public boolean updateDB(){
		String query = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
		try {
			ps = connection.prepareStatement(query);
			ps.executeUpdate();
			query = "INSERT INTO a2.mostPopulousCountries " +
				"(SELECT cid, population FROM a2.country WHERE population > 100000000 ORDER BY cid)";

			String checkQuery = "SELECT count(cid) FROM a2.mostPopulousCountries";

			ps = connection.prepareStatement(query);
			int inserted = ps.executeUpdate();
			ps = connection.prepareStatement(checkQuery);
			rs = ps.executeQuery();
			rs.next();
			if (rs.getInt("count") == inserted){
				return true;
			}
			return false;
		} catch (SQLException e) {
			return false;    
		}
		finally{
			try{
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
	}
}
