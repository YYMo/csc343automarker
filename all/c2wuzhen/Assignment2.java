
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
	  } catch (ClassNotFoundException e) {
		  return;
	  }
	  
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try{
		  connection = DriverManager.getConnection("jdbc:postgresql://"+URL, username, password);
	  } catch (SQLException e){
	  }
	  
	  if (connection != null) {
		  return true;
	  } else {
		  return false;
	  }
      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try{
		  connection.close();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }  
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		  String sqlText = "INSERT INTO a2.country VALUES (?,?,?,?)";
		  ps = connection.prepareStatement(sqlText);
		  ps.setInt(1,cid);
		  ps.setString(2,name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try {
		  sql = connection.createStatement();
		  String sqlText = "SELECT count(*) AS total FROM a2.oceanAccess"
		  		+ " GROUP BY oid HAVING oid = " + String.valueOf(oid);
		  rs = sql.executeQuery(sqlText);
		  if (rs.next()) {
			  int answer = rs.getInt(1);
			  rs.close();
			  return answer;
		  } else {
			  rs.close();
			  return -1;
		  }
		  
	  } catch (SQLException e) {
		  return -1;
	  }
  }
   
  public String getOceanInfo(int oid){
	  try {
		  sql = connection.createStatement();
		  String sqlText = "SELECT * FROM a2.ocean WHERE oid = " + String.valueOf(oid);
		  rs = sql.executeQuery(sqlText);
		  if (rs.next()) {
			  String answer = String.valueOf(rs.getInt(1)) + ":" + rs.getString(2)
					  + ":" + String.valueOf(rs.getInt(3));
			  rs.close();
			  return answer;
		  } else {
			  rs.close();
			  return "";
		  }
	  } catch (SQLException e) {
		  return "";
	  }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		  sql = connection.createStatement();
		  String sqlText = "UPDATE a2.hdi SET hdi_score = '" + String.valueOf(newHDI) +
				  "' WHERE cid = " + String.valueOf(cid) +
				  " AND year = " + String.valueOf(year);
		  if (sql.executeUpdate(sqlText) == 0) {
			  return false;
		  }
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		  sql = connection.createStatement();
		  String sqlText1 = "DELETE FROM a2.neighbour WHERE country = " + String.valueOf(c1id) +
				  " AND neighbor = " + String.valueOf(c2id);
		  String sqlText2 = "DELETE FROM a2.neighbour WHERE country = " + String.valueOf(c2id) +
				  " AND neighbor = " + String.valueOf(c1id);
		  if (sql.executeUpdate(sqlText1) == 0) {
			  return false;
		  }
		  if (sql.executeUpdate(sqlText2) == 0) {
			  return false;
		  }
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }      
  }
  
  public String listCountryLanguages(int cid){
	  try {
		  sql = connection.createStatement();
		  String answer = "";
		  String sqlText = "SELECT lid, lname, population*lpercentage/100 as pop "
		  		+ "FROM a2.country JOIN a2.language ON a2.country.cid = a2.language.cid "
		  		+ "WHERE country.cid = " + String.valueOf(cid) + " ORDER BY pop";
		  rs = sql.executeQuery(sqlText);
		  if (rs.next()) {
			  answer += String.valueOf(rs.getInt(1)) + ":" + rs.getString(2) + ":"
			  + String.valueOf(rs.getInt(3)) + "#";
			  while (rs.next()) {
				  answer += String.valueOf(rs.getInt(1)) + ":" + rs.getString(2) + ":"
						  + String.valueOf(rs.getInt(3)) + "#";
			  }
			  return answer.substring(0, answer.length()-1);
		  }
		  else {
			  return "";
		  }
	  } catch (SQLException e) {
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
		  sql = connection.createStatement();
		  String sqlText = "UPDATE a2.country SET height = " + String.valueOf(decrH)
				  + " WHERE cid = " + String.valueOf(cid);
		  if (sql.executeUpdate(sqlText) == 0) {
			  return false;
		  }
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
  }
    
  public boolean updateDB(){
	  try {
		  sql = connection.createStatement();
		  String table = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries("
		  		+ "cid INTEGER, cname VARCHAR(20))";
		  String sqlText = "INSERT INTO a2.mostPopulousCountries("
				  + "SELECT cid, cname FROM a2.country "
				  + "WHERE population > 1000"
				  + "ORDER BY cid ASC)";
		  sql.executeUpdate(table);
		  if (sql.executeUpdate(sqlText) == 0) {
			  return false;
		  }
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }  
  }
  
}
