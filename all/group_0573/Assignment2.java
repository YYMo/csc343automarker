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
	  System.out.println("-------- PostgreSQL JDBC Connection Testing -----------");
	  try {
		  // Load JDBC driver
		  Class.forName("org.postgresql.Driver");
	  } catch (ClassNotFoundException e){
		  System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
		  e.printStackTrace();
		  return;
	  }
	  System.out.println("PostgreSQL JDBC Driver Registered!");
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
    	  //Make the connection to the database, <username>
    	  System.out.println("URL:" + URL);
    	  System.out.println("username:" + username);
    	  System.out.println("password:" + password);
    	  System.out.println("Link Start!!!!!!!!!!!!!");
    	  connection = DriverManager.getConnection(URL,username,password);
    	  setPath();
      } catch (SQLException e) {
    	  System.out.println("Connection Failed. Check output console");
    	  e.printStackTrace();
    	  return false;
      }
	  return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() {
 
      try{
    	  connection.close();
      } catch (SQLException e) {
    	  System.out.println("Query exection Failed...");
    	  e.printStackTrace();
    	  return false;
      }
      return true;
  }
  
  public void setPath (){
	try {
		this.sql = connection.createStatement();
		sql.executeUpdate("SET search_path TO a2");
	} catch (SQLException e) {
		System.out.println("fail to set search path");
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population){
	  int output;
	  String sqlText;
	  
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }  
	  sqlText = "INSERT INTO country " + 
			  		"VALUES (" + (cid) + ", '" + name + "',"
			  		+ (height) + "," + (population)
			  		+ ")";
	  
	  System.out.println(sqlText);
	  try{
		  output = sql.executeUpdate(sqlText);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int output;
	  String sqlText;
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText = "SELECT count(cid) AS total_cid " +
	  			"From oceanAccess " +
	  			"WHERE oid =" + (oid);
	  try{
		  rs = sql.executeQuery(sqlText); 
	  } catch (SQLException e) {
		  System.out.println("Fail to executing query.");
		  e.printStackTrace();
		  return -1;
	  }
	  if (rs != null) {
		  try{
			  rs.next();
			  output = rs.getInt("total_cid");
		  } catch (SQLException e) {
			  System.out.println("Fail to getting rs.");
			  e.printStackTrace();
			  return -1;
		  }
		  return output;
	  }
	  return -1;
  }
   
  public String getOceanInfo(int oid){
	  String output;
	  String sqlText;
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText = "SELECT * " +
	  			"From ocean " +
	  			"Where oid =" + (oid);
	  try{
		  rs = sql.executeQuery(sqlText); 
	  } catch (SQLException e) {
		  System.out.println("Fail to executing query.");
		  e.printStackTrace();
		  return "";
	  }
	  if (rs != null) {
		  try{
			  rs.next();
			  output = (rs.getInt("oid")) + ":" +
					  rs.getString("oname") + ":" +
					  (rs.getInt("depth"));
		  } catch (SQLException e) {
			  System.out.println("Fail to getting rs.");
			  e.printStackTrace();
			  return "";
		  }
		  return output;
	  }
	  return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  int output;
	  String sqlText;
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText = "UPDATE hdi " +
			  		"SET hdi_score = " + (newHDI) + 
			  		" WHERE year = " + (year) + 
			  		" AND cid = " + (cid);
	  try{
		  output = sql.executeUpdate(sqlText);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  int output;
	  int output2;
	  String sqlText1;
	  String sqlText2;
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText1 = "DELETE FROM neighbour " +
			  		" WHERE country = " + (c1id) + 
			  		" AND neighbor = " + (c2id);
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText2 = "DELETE FROM neighbour " +
		  		" WHERE country = " + (c2id) + 
		  		" AND neighbor = " + (c1id);
	  try{
		  output = sql.executeUpdate(sqlText1);
		  output2 = sql.executeUpdate(sqlText2);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  return true;
           
  }
  
  public String listCountryLanguages(int cid){
	  String output = "";
	  String sqlText;
	  int n = 1;
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  sqlText = "SELECT lid, lname, (population*lpercentage) AS lpopulation " +
	  			"From language, country " +
	  			"Where country.cid = language.cid AND language.cid = " + (cid);
	  try{
		  rs = sql.executeQuery(sqlText); 
		  while (rs.next()) {
			  output += (rs.getInt("lid")) + ":" + 
					  rs.getString("lname") + ":" + 
					  (rs.getInt("lpopulation")) + "#";
			  n = n + 1;
			}
	  } catch (SQLException e) {
		  System.out.println("Fail to executing query.");
		  e.printStackTrace();
		  return "";
	  }
	  
	  
	  return output;

  }
  
  public boolean updateHeight(int cid, int decrH){
	  int output;
	  String sqlText;
	  
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  
	  sqlText = "UPDATE country " +
			  		"SET height = height - " + (decrH) + 
			  		" WHERE cid = " + (cid);
	  try{
		  output = sql.executeUpdate(sqlText);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  return true;
  }
    
  public boolean updateDB(){
	  String sqlText;
	  int output;
	  
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  
	  sqlText= "CREATE TABLE mostPopulousCountries("
              + "cid INTEGER,"
              + "cname   VARCHAR(20)"
              + ")";
	  try{
		  output = sql.executeUpdate(sqlText);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  
	  try{
		this.sql = connection.createStatement();
	  } catch (SQLException e) {
		  System.out.println("fail to create statement");
	  }
	  
	  sqlText = "INSERT INTO mostPopulousCountries("
              + "SELECT cid, cname " + "FROM country "
              + "WHERE population >= 100000000 "
              + "ORDER BY cid ASC "
              + ")";
	  try{
		  output = sql.executeUpdate(sqlText);
	  } catch (SQLException e) {
		  System.out.println("");
		  e.printStackTrace();
		  return false;
	  }
	  
	return true;    
  }
}


