import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Assignment2 {
	
	// A connection to the database  
	Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  // SQL command in string format
  String sqlText;
  
  //CONSTRUCTOR
  Assignment2(){
	  try {
		Class.forName("org.postgresql.Driver");
	} catch (Exception e) {
		System.exit(0);
	}
  }
  
  public boolean connectDB(String URL, String username, String password){
      //good
	  try{
    	  connection = DriverManager.getConnection(URL, username, password);
    	  sql.executeUpdate("SET search_path to A2");
    	  if (connection == null) {
    		  return false;
    	  }
    	  return true;
      } 
      catch (Exception e) {
    	  return false;
      }
  }
  
  public boolean disconnectDB(){
	  try{
    	  connection.close();
    	  return true;
      }
      catch (Exception e){
    	  return false;  
      }   
  }
  
  public boolean insertCountry (int cid, String name, int height, int population) {
	  //good
	  try{
		  sqlText = "INSERT INTO country (cid, cname, height, population) VALUES " +
		  		"(" + cid + ", '" + name + "', " + height + ", " + population + ");";
	      sql.executeUpdate(sqlText);
		  return true;
	  }
	  catch (Exception e){
		  return false;  
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  //good
	  try{
		  int numCountries = 0;
		  sqlText = "SELECT oid FROM oceanAccess WHERE oid = "+oid+";";
		  sql = connection.createStatement();
		  rs = sql.executeQuery(sqlText);
		  while (rs.next()){
			  numCountries++;
		  }
		  rs.close();	
		  sql.close();
		  return numCountries;
	  }
	  catch (Exception e){
		  return -1;  
	  }
  }
   
  public String getOceanInfo(int oid){
	  //good
	  try{
		  String OceanInfo = "";
		  sqlText = "SELECT oid,oname,depth FROM ocean WHERE oid="
				  + oid + ";";
		  sql = connection.createStatement();
		  rs = sql.executeQuery(sqlText);
		  while (rs.next()){
			  OceanInfo += rs.getInt("oid")+":";
			  OceanInfo += rs.getString("oname")+":";
			  OceanInfo += rs.getInt("depth");
		  }
		  rs.close();	
		  sql.close();
		  return OceanInfo;
	  }
	  catch (Exception e){
		  return "";  
	  }
}

  public boolean chgHDI(int cid, int year, float newHDI){
	  try{
		  sqlText = "UPDATE hdi SET hdi_score = " + newHDI +
		  		" WHERE cid = " + cid + " AND year = " + year + ";";
		  sql = connection.createStatement();
		  sql.executeUpdate(sqlText);
		  sql.close();  
		  return true;
	  }
	  catch (SQLException e){
		   return false;
	  }
  }
  
  public String listCountryLanguages(int cid){
	  //good enough
	  try{
		  sqlText = "SELECT l.lid AS lid c.cname AS lname" +
		  		"SUM(l.lpercentage * c.population) AS population " +
		  		"FROM country c JOIN language l ON l.cid = c.cid " +
		  		"WHERE c.cid = " + cid + " " +
		  		"GROUP BY l.lid, c.cname " +
		  		"ORDER BY population;";
		  String countryLang = "";
		  sql = connection.createStatement();
		  rs = sql.executeQuery(sqlText);
		  while (rs.next()){
			  countryLang += rs.getInt("cid") + ":";
			  countryLang += rs.getString("lname") + ":";
			  countryLang += rs.getFloat("population");
			  if (rs.next()) {countryLang += "#";}
		  }
		  rs.close();	
		  sql.close();
		  return countryLang;
	  }
	  catch (SQLException e){
		  return "";  
	  }
  }
  
  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
		  sqlText = "DELETE FROM neighbour " +
		  		"WHERE country = " + c1id + " neighbor = " + c2id + " OR "+
		  		"country = " + c2id + " neighbor = " + c1id + ";";
		  sql = connection.createStatement();
		  sql.executeUpdate(sqlText);
		  sql.close();
		  return true;
	  }
	  catch (Exception e){
		  return false;  
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try{
		  int height = -1;
		  sql = connection.createStatement();
		  sqlText = "SELECT height FROM country WHERE cid = " + cid + ";";
		  rs = sql.executeQuery(sqlText);
		  height = rs.getInt("height");
		  rs.close();
		  if (height == -1){return false;}
		  sqlText = "UPDATE country SET height = " + (height - decrH) +
			  		" WHERE cid = " + cid + ";";
		  sql.executeUpdate(sqlText);
		  sql.close();  
		  return true;
	  }
	  catch (SQLException e){
		   return false;
	  }
  }
    
  public boolean updateDB(){
	  try{
		  sql = connection.createStatement();
		  sqlText = "CREATE TABLE mostPopulousCountries (cid INTEGER(country ID)," +
		  		" cname VARCHAR(20) (country name));";		  
		  sql.executeUpdate(sqlText);
		  sqlText = "INSERT INTO mostPopulousCountries(SELECT cid, cname FROM" +
		  		"country WHERE population > 100000000);";
		  sql.executeUpdate(sqlText);
		  sql.close();
		  return true;
	  }
	  catch (SQLException e){
		  return false;  
	  }   
  } 
}
