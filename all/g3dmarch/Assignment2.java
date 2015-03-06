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
			//prepare statement
			ps = connection.prepareStatement(query);

			//set the values
			ps.setInt(1, cid);
			ps.setString(2, name);
			ps.setInt(3, height);
			ps.setInt(4, population);

			//update query
			ps.executeUpdate();

			//check if country is inserted
			String query2 = "SELECT cid FROM a2.country WHERE cid = ?";

			ps = connection.prepareStatement(query2);
			ps.setInt(1, cid);
			rs = ps.executeQuery();

			//return if any value exists in table (success)
			return rs.next();
		} 
		catch (SQLException e) {
			return false;
		}
		finally{
			try{

				//close connection/statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
	}

	public int getCountriesNextToOceanCount(int oid) {

		//count the number of countries near ocean
		String query = "SELECT count(cid) AS num FROM a2.oceanAccess GROUP BY oid HAVING oid=?";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);
			rs = ps.executeQuery();

			//move to first row
			rs.next();

			//return the value in the first column
            return rs.getInt(1);
		} 
		catch (SQLException e) {
			return -1;
		}
		finally{
			try{

				//close connection/statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		
	}

	public String getOceanInfo(int oid){

		// get the full tuple from ocean with provided oid
		String query = "SELECT * FROM a2.ocean WHERE oid=?";
		try {

			//prepare statement
			ps = connection.prepareStatement(query);
			ps.setInt(1, oid);

			//exectue query
			rs = ps.executeQuery();

			//if the ocean exists
			if (rs.next()){

				//get its information from the table
				int id = rs.getInt(1);
				String oname = rs.getString(2);
				int depth = rs.getInt(3); 

				//return the information in the format id:oname:depth
				return id+":"+oname+":"+ depth;
			}
			else {

				//if no ocean found return ""
				return "";
			}
		} 
		catch (SQLException e) {
			return "";
		}
		finally{
			try{

				//close connection/statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		   
	}

	public boolean chgHDI(int cid, int year, float newHDI){

		//change HDI value using given information about country
		String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
		try {

			//prepare statement
			ps = connection.prepareStatement(query);

			//set values
			ps.setFloat(1, newHDI);
			ps.setInt(2, cid);
			ps.setFloat(3, year);

			//change the HDI
			ps.executeQuery();

			//second query to check if update was successful
			String query2 = "SELECT hdi_score FROM a2.hdi WHERE cid = ? AND year = ?";
			ps = connection.prepareStatement(query2);
			rs = ps.executeQuery();

			//move to first row
			rs.next();

			//compare newHDI to the HDI value in the query2 table
			return newHDI == rs.getInt("hdi_score");
		} catch (SQLException e) {
			return false;
		}
		finally{
			try{

				//close statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
	
	}

	public boolean deleteNeighbour(int c1id, int c2id){

		//delete both neighbours form the table, (c1id,c2id) and (c2id,c1id)
	   String query = "DELETE FROM a2.neighbour WHERE country=? AND neighbor=?";
	   try {

	   		//prepare statement
			ps = connection.prepareStatement(query);

			//delete first tupple (c1id, c2id) from neighbour
			ps.setInt(1, c1id);
			ps.setInt(2, c2id);
			ps.executeUpdate();

			//delete second tupple (c2id, c1id) from neighbour
			ps = connection.prepareStatement(query);
			ps.setInt(2, c1id);
			ps.setInt(1, c2id);
			ps.executeQuery();

			//check if first delete succeded
			String querycheck = "SELECT * FROM a2.neighbour WHERE country=? AND neighbor=?";
			ps = connection.prepareStatement(querycheck);
			ps.setInt(1, c1id);
			ps.setInt(2,c2id);
			rs = ps.executeQuery();

			//if table empty, return false
			if (rs.next()){
				return false;
			}

			//check if second delete succeded
			ps = connection.prepareStatement(querycheck);
			ps.setInt(2, c1id);
			ps.setInt(1, c2id);
			rs = ps.executeQuery();

			//if table empty, return false
			if (rs.next()){
				return false;
			}

			//if everything succeeded return true
			return true;  
		} catch (SQLException e) {
			return false;
		}
		finally{
			try{

				//close statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		         
	}

	public String listCountryLanguages(int cid){

		// select relevant information from country nat join language for given country cid
		String query = "SELECT lid, cname, population, lpercentage FROM a2.country" +
				" NATURAL JOIN a2.language WHERE cid=? ORDER BY population";
		try {
			ps = connection.prepareStatement(query);
			ps.setInt(1, cid);
			rs = ps.executeQuery();

			// create empty builder string
			String result = "";

			// loop until rs.next is NULL, for each row
			while (true){
				if (rs.next()){
					// get the information and add it to the builder string
					result = result + rs.getInt("lid") + ":";
					result = result + rs.getString("cname") + ":";
					int population = rs.getInt("population");
					float lpercentage = rs.getFloat("lpercentage");

					//compute number of people that speak the language
					float language_speakers = population * lpercentage;
					result = result + language_speakers + "#";
				}
				else{
					//finish loop and return the result
					return result;
				}
			}
		} 
		catch (SQLException e) {
			return "";
		}
		finally{
			try{
				//close statement
				ps.close();
				rs.close();
			}
			catch (SQLException e){
			}
		}
		   
	}

	public boolean updateHeight(int cid, int decrH){

		// find the height of the country with cid
		String query1 = "SELECT HEIGHT FROM a2.country WHERE cid = ?";

		// update the height of the with height from query1 - decrH
		String query2 = "UPDATE a2.country SET height = ? WHERE cid = ?";
		try {
			ps = connection.prepareStatement(query1);
			ps.setInt(1, cid);
			rs = ps.executeQuery();

			// if country doesnt exist, return false
			if (!rs.next()){
				return false;
			}

			// current height of country
			int height = rs.getInt("height");

			// new decremented height
			int updatedHeight = height - decrH;

			//prepare updated statement
			ps = connection.prepareStatement(query2);
			ps.setInt(1, updatedHeight);
			ps.setInt(2, cid);

			//update height
			ps.executeUpdate();

			// third query to check if height is updated
			String query3 = "SELECT height FROM a2.country WHERE cid = ?";
			ps = connection.prepareStatement(query3);
			ps.setInt(1, cid);
			rs = ps.executeQuery();

			// check if updatedHeight is the new height
			return updatedHeight == rs.getInt("height");

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

		// create a new table mostPopuluosCountries with attributes cid and cname
		String query1 = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
		try {
			ps = connection.prepareStatement(query1);
			ps.executeUpdate();

			// insert into the new table all countries from table country with pop > 1 million
			String query2 = "INSERT INTO a2.mostPopulousCountries " +
				"(SELECT cid, population FROM a2.country WHERE population > 100000000 ORDER BY cid ASC)";

			// find the number of countries in mostPopulousCountries table
			String query3 = "SELECT count(cid) FROM a2.mostPopulousCountries";

			ps = connection.prepareStatement(query2);

			// get the number of rows inserted with from query2
			int inserted = ps.executeUpdate();
			ps = connection.prepareStatement(query3);
			rs = ps.executeQuery();
			rs.next();

			// if the number of rows inserted == number of countries in mostPop
			if (rs.getInt("count") == inserted){
				return true;
			}

			//if failed return false.
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
