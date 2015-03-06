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
  public static void main(String argc[]){
	  Assignment2 a2 = new Assignment2();
  }
  Assignment2(){
	  try {
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		connection = DriverManager.getConnection(URL,username,password);
		return true;
	} catch (SQLException e) {
		e.printStackTrace();
	}
      return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try {
		connection.close();
	} catch (SQLException e) {
		e.printStackTrace();
	}
      return false;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		  String test = "select cid from country where cid = ?;";
		  PreparedStatement pstest = connection.prepareStatement(test);
		  pstest.setInt(1, cid);
		  ResultSet rstest = pstest.executeQuery();
		  if (!rstest.next()){
			  String query = "insert into country(cid, cname, height, " +
					  "population) values(?, ?, ?, ?);";
			  ps = connection.prepareStatement(query);
			  ps.setInt(1, cid);
		      ps.setString(2, name);
		      ps.setInt(3, height);
		      ps.setInt(4, population);
		      ps.executeUpdate();
		      return true;
		  }
		  
	  } catch (SQLException e) {
		e.printStackTrace();
	  }
	  return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try{
		 String query = "select count(cid) from oceanAccess where oid = ?;";
		 ps = connection.prepareStatement(query);
		 ps.setInt(1, oid);
		 rs = ps.executeQuery();
		 rs.next();
		 return rs.getInt(1);
	  }catch(SQLException e){
		  e.printStackTrace();
	  }
	  return -1;  
  }
  
  public String getOceanInfo(int oid){
	  try{
		  String query = "select oid, oname, depth from ocean where oid = ?;";
		  ps = connection.prepareStatement(query);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  rs.next();
		  String answer = rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
		  return answer;
	  }catch(SQLException e){
		  e.printStackTrace();
	  }
	  return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try{
		  String query = "update hdi set hdi_score = ? where cid = ? and year = ?;";
		  ps = connection.prepareStatement(query);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  ps.setFloat(1, newHDI);
		  ps.executeUpdate();
		  return true;
	  }catch(Exception e){
		  e.printStackTrace();
	  }
	  return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
		  String query = "delete from neighbour where (country = ? and neighbor = ?) " +
		  		"or (country = ? and neighbor = ?);";
		  ps = connection.prepareStatement(query);
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
		  ps.setInt(3, c2id);
		  ps.setInt(4, c1id);
		  ps.executeUpdate();
		  return true;
	  }catch(SQLException e){
		  e.printStackTrace();
	  }
	  return false;        
  }
  
  public String listCountryLanguages(int cid){
	  try{
		  String query = "select cid from language where cid = ?";
		  ps = connection.prepareStatement(query);
		  ps.setInt(1, cid);
		  ps.executeUpdate();
		  return; 
	  }catch(SQLException e){
		  e.printStackTrace();
	  }
	  return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try{
		  String query = "update country set height = " +
		  		"(select height from country where cid = ?) - ? where cid = ?;";
		  ps = connection.prepareStatement(query);
		  ps.setInt(1, cid);
		  ps.setInt(2,decrH);
		  ps.setInt(3, cid);
		  ps.executeUpdate();
		  return true;
	  }catch(Exception e){
		  e.printStackTrace();
	  }
	  return false;
  }
    
  public boolean updateDB(){
	  try{
		  String query = "create view pop_con as select cid, cname from country where population > 100000000; " +
		  		"create table mostPopulousCountries(cid INTEGER REFERENCES country(cid), " +
		  		"cname VARCHAR(20) NOT NULL, PRIMARY KEY(cid)); insert into mostPopulusCountries " +
		  		"select * from pop_con;";
		  ps = connection.prepareStatement(query);
		  ps.executeUpdate();
		  return true;
	  }catch(SQLException e){
		  e.printStackTrace();
	  }
	  return false;    
  }
  
}
