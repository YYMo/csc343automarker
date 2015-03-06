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
		try {
			Class.forName("org.postgresql.Driver");
			
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			//System.out.println("Failed to find the JDBC driver "+ e.getMessage());
		}
	}


	//Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password){
		try{
			connection = DriverManager.getConnection(URL, username, password);    

			return true;
		}
		catch (SQLException e){
			//System.out.println("Connection Error "+ e.getMessage() + "\n");
			e.printStackTrace();
			return false;
		} 
		


	}

	//Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB(){
		try{
			connection.close();      
			rs.close();
			ps.close();
			return true;    
		}
		catch(SQLException e){
			//System.out.println("Disconnection Error "+ e.getMessage());
			e.printStackTrace();
			return false;

		}
	}


	public boolean insertCountry (int cid, String name, int height, int population) {

		String insertC = "INSERT INTO a2.country " +
		"(cid,cname,height,population) VALUES " +
		"(?,?,?,?)";
		try{

			ps = connection.prepareStatement(insertC);

			ps.setInt(1,cid);
			ps.setString(2, name);
			ps.setInt(3,height);
			ps.setInt(4,population);

			ps.executeUpdate ();
		     
			return true;

		}
		catch(SQLException e){
			e.printStackTrace();
			//System.out.println("INSERT Error "+ e.getMessage());
			return false;
		}

	}

	public int getCountriesNextToOceanCount(int oid) {
		String getCountriesNexttoOceans = "SELECT oid from a2.oceanAccess where oid=?";
		try{
			ps = connection.prepareStatement(getCountriesNexttoOceans);

			ps.setInt(1, oid);

			ResultSet rs = ps.executeQuery();
			int count = 0;
			while(rs.next()){
				count++;
			}
			return count;
		}
		catch (SQLException e){
			//System.out.println("getCountriesNextToOceanCount error"+ e.getMessage());
			e.printStackTrace();
			return -1;
		}
	}

	public String getOceanInfo(int oid){
		String ans = "";
		String oceanIn = "SELECT oid,oname,depth FROM a2.ocean WHERE oid=?";
		try{
			ps = connection.prepareStatement(oceanIn);
			ps.setInt(1,oid);
			rs = ps.executeQuery();
			rs.next();
			
			int newoid = rs.getInt("OID");
			String oname = rs.getString("oname");
			int depth = rs.getInt("depth");

			ans =newoid + ":"+oname + ":"+depth;
			return ans;
		}
		catch(SQLException e){
			//System.out.println("OCEAN ERR "+ e.getMessage());
			e.printStackTrace();
			return "";
		}
	}

	public boolean chgHDI(int cid, int year, float newHDI){
		String changeH = "UPDATE a2.hdi set hdi_score = ? where year = ? AND cid =?";
		try{
			ps = connection.prepareStatement(changeH);

			ps.setFloat(1, newHDI);
			ps.setInt(2,year);
			ps.setInt(3,cid);

			ps.executeUpdate ();
			return true;
		}
		catch (SQLException e){
			//System.out.println("change HDI error "+ e.getMessage());
			e.printStackTrace();
			return false;

		}
	}

	public boolean deleteNeighbour(int c1id, int c2id){
		String deleteN = "DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?";
		try{
			ps = connection.prepareStatement(deleteN);

			ps.setInt(1, c1id);
			ps.setInt(2,c2id);

			ps.executeUpdate();

			String deleteN2 = "DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?";

			ps = connection.prepareStatement(deleteN2);

			ps.setInt(1, c2id);
			ps.setInt(2, c1id);

			ps.executeUpdate();
			return true;
		}
		catch (SQLException e){
			//System.out.println("delete Neighbour error "+ e.getMessage());
			e.printStackTrace();
			return false;        
		}
	}

	public String listCountryLanguages(int cid){
		String ans = "";
		String listCountryLang = "SELECT language.lid, language.lname, language.lpercentage * country.population as population" +
		" FROM a2.language join a2.country using (cid) where cid = ?";
		try{
			ps = connection.prepareStatement(listCountryLang);
			ps.setInt(1,cid);

			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ans = ans + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population") + "#";
			}

			if (ans.length() > 0 && ans.charAt(ans.length()-1)=='#') {
				ans = ans.substring(0, ans.length()-1);
			}
			return ans;
		}
		catch (SQLException e){
			//System.out.println("list country languages error "+ e.getMessage());
			e.printStackTrace();
			return "";
		}
	}

	public boolean updateHeight(int cid, int decrH){
		String updateH = "SELECT cid,cname,height,population from a2.country where cid=?";
		try{
			ps = connection.prepareStatement(updateH);
			ps.setInt(1,cid);

			ResultSet rs = ps.executeQuery();
			int height =0;
			if (rs!=null){
				while(rs.next()){
					rs.getInt("cid");
					rs.getString("cname");
					height = rs.getInt("height");
					rs.getInt("population");
				}

				String q2 = "UPDATE country SET height = ? WHERE cid = ?";
				ps = connection.prepareStatement(q2);
				ps.setInt(1, height-decrH);
				ps.setInt(2,cid);
				return true;
			}
			else
				return false;
		}
		catch (SQLException e){
			//System.out.println("update Height error "+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public boolean updateDB(){
		//remakes the table every time updateDB is called
		try{
			String oldDelete = "DROP TABLE IF EXISTS a2.mostPopulousCountries";
			ps = connection.prepareStatement(oldDelete);
			ps.executeUpdate();

			String newTable = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL, PRIMARY KEY (cid))";
			ps = connection.prepareStatement(newTable);
			ps.executeUpdate();
		} 
		catch (SQLException e) {
			//System.out.println("Update Database error "+ e.getMessage());
			e.printStackTrace();
			return false;
		}
		String populous ="SELECT cid,cname FROM a2.country WHERE country.population > 700000";

		try{
			ps = connection.prepareStatement(populous);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				int cid1 = rs.getInt("cid");
				String cname = rs.getString("cname");

				String q3= "INSERT INTO a2.mostPopulousCountries(cid,cname) VALUES (?,?)";
				ps = connection.prepareStatement(q3);
				ps.setInt(1,cid1);
				ps.setString(2,cname);
				ps.executeUpdate();
			}
			return true;    
		}
		catch(SQLException e){
			//System.out.println("Update Database error "+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

}