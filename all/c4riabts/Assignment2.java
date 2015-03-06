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
	  try{
		  Class.forName("org.postgresql.Driver");
	  } catch(ClassNotFoundException e){
		  
	  }  
  }
  
  
  /*public static void main(String[] argv){
	  //System.out.println("Initial Success");
  }*/
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successfull
  public boolean connectDB(String URL, String username, String password){
	  try{
		  connection = DriverManager.getConnection(URL, username, password);
	  } catch(SQLException e){
		  return false;
	  }
	  //I refuse to check for null... I mean what are exceptions for?
	  
      return true;
  }
  
  //Closes the connection. Returns true if closure was successfull
  public boolean disconnectDB(){  
	  try{
		  connection.close();
	  } catch(SQLException e){
		  return false;
	  }
      return true;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  //I don't have to check for existance, that causes an exception
	  String sqlText = "INSERT INTO country VALUES(?, ?, ?, ?)";
  
	  try{
		  ps = connection.prepareStatement(sqlText);
		  
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  
		  ps.executeUpdate(sqlText);
		  
		  ps.close();
		  
	  } catch(SQLException e){
		  return false;
	  }
	  return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  String sqlText =	"SELECT COUNT(oA.cid)"
	  				+ 	"FROM oceanAccess oA"
	  				+ 	"WHERE oA.oid = ?";  
	  int count;
	  
	  try{
		  ps = connection.prepareStatement(sqlText);
		  
		  ps.setInt(1, oid);
 
		  rs = ps.executeQuery(sqlText);
		  
		  if(rs != null)
			  count = rs.getInt(1);
		  else
			  count = 0;
		  
		  rs.close();
		  ps.close();
		  
	  } catch(SQLException e){
		  return -1;
	  }
	  return count;

  }
  
  public String getOceanInfo(int oid){
	  String sqlText =	"SELECT o.oid, o.oname, o.depth"
	  				+ 	"FROM ocean o"
	  				+ 	"WHERE o.oid = ?";
  
	  String result;

	  try{
		  ps = connection.prepareStatement(sqlText);

		  ps.setInt(1, oid);

		  rs = ps.executeQuery(sqlText);
		  
		  if(rs != null)
			  result = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
		  else
			  result = "";

		  rs.close();
		  ps.close();

	  	} catch(SQLException e){
	  		return "";
	  	}
	  return result;

  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  String sqlText = 	"UPDATE hdi SET hdi_score = ? "
	  				+ 	"WHERE cid = ? AND year = ?";
	  
	  try{
		  ps = connection.prepareStatement(sqlText);
		  
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  
		  ps.executeUpdate(sqlText);
		  
		  ps.close();
		  
	  } catch(SQLException e){
		  return false;
	  }
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  String sqlText = 	"DELETE FROM neighbour WHERE country = ? OR country = ?";

	  try{
		  ps = connection.prepareStatement(sqlText);
		  
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
		  
		  ps.executeUpdate(sqlText);
		  
		  ps.close();
	  
	  } catch(SQLException e){
		  return false;
	  }
	  return true;
  }
  
  public String listCountryLanguages(int cid){
	  String sqlText;

	  String result = "";
	  
	
	  try{
		  sqlText = "SELECT c.population FROM country c WHERE c.cid = ?";
		  ps = connection.prepareStatement(sqlText);
	
		  ps.setInt(1, cid);
	
		  rs = ps.executeQuery(sqlText);
		  
		  float population;
		  if(rs != null)
			  population = rs.getInt("population");
		  else
			  population = (float) -1.0;
	
		  rs.close();
		  ps.close();
		  if(population > 0.0){
			  sqlText = "SELECT l.lid, l.lname, l.lpercentage"
			  		+ 	"FROM language l"
			  		+ 	"WHERE l.cid = ?"
			  		+ 	"ORDER BY l.lpercentage DESC"; 
			  ps = connection.prepareStatement(sqlText);
				
			  ps.setInt(1, cid);
		
			  rs = ps.executeQuery(sqlText);
		  	  
			  if(rs != null){
				  while(rs.next()){
					  result += rs.getInt("lid") 
							  + ":" + rs.getString("lname") 
							  + ":" + (rs.getFloat("lpercentage") * population / 100.0) 
							  + "#";
				  }		  
			  }				   
			  else
				  result = "";
			  
			  rs.close();
			  ps.close();
		  }
		  else
			  result = "";
		  
		} catch(SQLException e){
			return "";
		}
	  return result;
	  
  }
  
  public boolean updateHeight(int cid, int decrH){
	  String sqlText = 	"UPDATE country SET height = height - ? "
					+ 	"WHERE cid = ?";

	  try{
		  ps = connection.prepareStatement(sqlText);
		  
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  
		  ps.executeUpdate(sqlText);
		  
		  ps.close();
	  
	  } catch(SQLException e){
	  return false;
	  }
	  
	 return true;
  }
    
  public boolean updateDB(){
	  String sqlText;	

	  try{
		  sqlText = "CREATE TABLE mostPopulousCountries ("
			    + 	"cid 		INTEGER" 	
			    +	"cname 		VARCHAR(20))";
		  ps = connection.prepareStatement(sqlText); 
		  ps.executeUpdate(sqlText);		  
		  ps.close();
		  
		  sqlText = "INSERT INTO mostPopulousCountries ("
		  		+ 	"SELECT c.cid, c.cname"
				+ 	"FROM country c"
				+ 	"WHERE c.population > 100e6"
				+ 	"ORDER BY c.cid ASC)";
		  ps = connection.prepareStatement(sqlText);
		  ps.executeUpdate(sqlText);
		  ps.close();

		  } catch(SQLException e){
			  return false;
		  }

	  return true;  
  }
  
}
