import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Result set for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	  try{
		  Class.forName("org.postgresql.Driver");
		  
	  }catch(ClassNotFoundException e){
		  System.out.println("PostgreSQL JDBC Driver not found");
		  e.printStackTrace();
		  return;
		  
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. 
  //Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
	  
      try{
    	  connection = DriverManager.getConnection(URL, username, password);
    	  return true;
    	  
      }catch(Exception e){
    	  System.err.println(e);
    	  return false;
      }
	  
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  
      try{
    	  connection.close();
    	  return true;
    	  
      }catch(SQLException e){
    	  System.out.println("Disconnect failed!");
    	  e.printStackTrace();
    	  return false;
      }
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
	  boolean exists = false;
	  boolean result = false;
	  String queryString;

	  try {
		  sql = connection.createStatement();
		  rs = sql.executeQuery("SELECT cid FROM a2.country");
		  while (rs.next()) {
			  if (cid == rs.getInt("cid")) {
				  exists = true;
				  break;
			  }
		  }
		  if (!exists) {
			  sql.close();
			  rs.close();
			  
			  try {
				  queryString = "INSERT INTO a2.country(cid, cname, height, population) VALUES (?,?,?,?);";
				  ps = connection.prepareStatement(queryString);
				  ps.setInt(1,cid);
				  ps.setString(2,name);
				  ps.setInt(3,height);
				  ps.setInt(4,population);
				  int success = ps.executeUpdate();

				  if (success == 1) {
					  result = true;
				  }

			  } catch (SQLException e) {
				  e.printStackTrace();
				  result = false;
			  }

		  }else{
			  sql.close();
			  rs.close();
			  result = false; 
		  }


	  } catch (SQLException e) {
		  e.printStackTrace();
		  result = false;
	  }

	  finally {
		  try {
			  if (ps!=null) {
				  ps.close();
			  }

		  } catch (SQLException e) {
			  e.printStackTrace();
			  result = false;
		  }
	  }
	  return result;
  }

  public int getCountriesNextToOceanCount(int oid) {
	  
	  try{
		  sql = connection.createStatement();
		  rs = sql.executeQuery("SELECT oid, count(DISTINCT cid) FROM a2.oceanAccess GROUP BY oid");
		  while (rs.next()) {
			  
			  if (oid == rs.getInt("oid")) {
				  return rs.getInt(2);
			  }
		  }
		  
		  rs.close();
		  sql.close();
		  return -1;
		  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return -1;
	  }
	  
  }
   
  public String getOceanInfo(int oid){
   
	  try{
		  sql = connection.createStatement();
		  ResultSet rs = sql.executeQuery("SELECT * FROM a2.ocean");
		  while (rs.next()) {
			  
			  if (oid == rs.getInt("oid")) {
				  return rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
			  }
		  }
		  
		  rs.close();
		  sql.close();
		  return "";
		  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return "";
	  }
	  
  }

  public boolean chgHDI(int cid, int year, float newHDI){
  
	  String queryString;
	  
	  try{
		  queryString = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ?, year = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setFloat(1,newHDI);
		  ps.setInt(2,cid);
		  ps.setInt(3,year);
		  int success = ps.executeUpdate();
		  
		  if (success == 1) {
			  sql.close();
			  rs.close();
			  ps.close();
			  return true;
		  }else{
			  sql.close();
			  rs.close();
			  ps.close();
			  return false;
		  }
	  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return false;
	  }
	  
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   
 String queryString;
	  
	  try{
		  queryString = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1,c1id);
		  ps.setInt(2,c2id);
		  
		  int success = ps.executeUpdate();
		  
		  if (success == 1) {
			  ps.close();
			  
			  queryString = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?";
			  ps = connection.prepareStatement(queryString);
			  ps.setInt(1,c2id);
			  ps.setInt(2,c1id);
			  
			  success = ps.executeUpdate();
			  
			  if (success == 1) {
				  ps.close();
				  return true;
			  }else{
				  ps.close();
				  return false;
			  }
		  }else{
			  ps.close();
			  return false;
		  }
	  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return false;
	  }
  }
  
  public String listCountryLanguages(int cid){
	  
	  String results = "";
	  int population = 0;
	  
	  try{
		  boolean exists = false;
		  sql = connection.createStatement();
		  ResultSet rs = sql.executeQuery("SELECT * FROM a2.country");
		  
		  while (rs.next()) {
			  
			  if (cid == rs.getInt("cid")) {
				  population = rs.getInt("population");
				  exists = true;
			  }
		  }
		  
		  if(!exists){
			  rs.close();
			  sql.close();
			  return "";
			  
		  }else{

			  sql = connection.createStatement();
			  rs = sql.executeQuery("SELECT * FROM a2.language ORDER BY population DESC");
			  
			  while (rs.next()) {
				  
				  if (cid == rs.getInt("cid")) {
					  
					  if(results.equals("")){
						  results = rs.getInt("lid") + ":" + rs.getString("lname") + ":" +
								(rs.getFloat("lpercentage") * population);  
					  }else{
						  results += "#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" +
								(rs.getFloat("lpercentage") * population); 
					  }

				  }
			  }
			  
			  rs.close();
			  sql.close();
			  return results;
		  }
		  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){

	  String queryString;
	  int height = 0;
	  
	  try{
		  boolean exists = false;
		  sql = connection.createStatement();
		  ResultSet rs = sql.executeQuery("SELECT * FROM a2.country");
		  
		  while (rs.next()) {
			  
			  if (cid == rs.getInt("cid")) {
				  height = rs.getInt("height");
				  exists = true;
			  }
		  }
		  
		  if(!exists){
			  rs.close();
			  sql.close();
			  return false;

		  }else{

			  queryString = "UPDATE a2.country SET height = ? WHERE cid = ?";
			  ps = connection.prepareStatement(queryString);
			  ps.setInt(1,height - decrH);
			  ps.setInt(2,cid);;
			  int success = ps.executeUpdate();

			  if (success == 1) {
				  sql.close();
				  rs.close();
				  ps.close();
				  return true;
			  }else{
				  sql.close();
				  rs.close();
				  ps.close();
				  return false;
			  }
		  }

	  }catch(SQLException e){
		  e.printStackTrace();
		  return false;
	  }
  }

  public boolean updateDB(){
	  
	  try{
		  sql = connection.createStatement();
		  sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries (" +
		  		"cid int, cname varchar(20))");
		  sql.executeUpdate("INSERT INTO a2.mostPopulousCountries(SELECT cid, cname FROM" +
		  		" a2.country WHERE population > 100000000);");
		  
		  sql.close();
		  return true;
		  
	  }catch(SQLException e){
		  e.printStackTrace();
		  return false;
	  }
  }
    
    public static void main(String[] args){
        
    }
}
