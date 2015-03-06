
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

		// Load JDBC Driver
		Class.forName("org.postgresql.Driver");

		}

	catch (ClassNotFoundException e) {	

	}
  }
  
  // private helper methods
  private void close() {
	  try {
		  if (ps != null && !ps.isClosed()) {
			  ps.close();
		  }
		
		  if (rs != null && !rs.isClosed()) {
			  rs.close();
		  }
	
	  }
	  catch (SQLException e) {
		  
	  }
	  
  }
  

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
		  ps = connection.prepareStatement("SET search_path TO a2");
		  ps.executeUpdate();

		  ps.close();
		
	  }
	  
	  catch (SQLException e) {
		  return false;
	  }
		  
	return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  
	  try {
		  connection.close();
	  }
	  
	  catch (SQLException e) {
		  return false;
	  }
	  
	  return true;
		       
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		  ps = connection.prepareStatement("SELECT cid FROM country");

		  rs = ps.executeQuery();
		
		  
		  while(rs.next()) {
			if (cid == rs.getInt(1)) {
				rs.close();
				ps.close();
				
				return false;
			}
			  
		  }
		  
		  ps.close();
		  rs.close();
		  
		  ps = connection.prepareStatement(
				  "INSERT INTO country(cid, cname, height, population) " +
				  "VALUES(?, ?, ?, ?)");
		  
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  
		  if (ps.executeUpdate() == 0) {
			close();
			return false;
		  }
		  
		  ps.close();
		  rs.close();
	  }
	  
	  catch (SQLException e) {
		  close();
		  return false;
	  }
	  
	  return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int count = 0;
	  
	  try {
		  ps = connection.prepareStatement(
				  "SELECT * FROM oceanAccess");
		  
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  if (oid == rs.getInt(2)){
				  count += 1;
			  }
		  }
		  
		 rs.close();
		 ps.close();
	  }
	  
	  catch (Exception e) {
		  close();
		  return -1;
	  }
	  
	  
	  return count;  
  }
   
  public String getOceanInfo(int oid){
	  String result = "";
	  
	  try {
		  ps = connection.prepareStatement(
				  "SELECT * FROM ocean");
		  
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  
			  if (oid == rs.getInt(1)) {
				  result = (String.valueOf(rs.getInt(1)) + ":" +
						  rs.getString(2) + ":" +
						  String.valueOf(rs.getInt(3)));
			  
			  }
			  
		  }
		  
		  rs.close();
		  ps.close();
	  }
	  catch (SQLException e) {
		 close();
	  }
	  
   return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  
	  if (newHDI < 0 || newHDI > 1 ) {
		  return false;
	  }
	  
	  try {
	
		  ps = connection.prepareStatement("UPDATE hdi" +
		  		" SET hdi_score = ? WHERE cid = ? and year = ?");
		  
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  
		  if (ps.executeUpdate() == 0) {
			  close();
			  return false;
		  }
		  
		  ps.close();
	  }
	  
	  catch (SQLException e) {
		  close();
		  return false;
	  }
	  
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  
	  try {
		  ps = connection.prepareStatement(
				  "DELETE FROM neighbour WHERE country = ? and neighbor = ?");
		  
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);

		  ps.executeUpdate();

		  ps.setInt(1, c2id);
		  ps.setInt(2, c1id);
		  
		  ps.executeUpdate();
		  
		  ps.close();
	  
	  }
	  catch (SQLException e) {
		  close();
		  return false;
	  }
	  
	  return true;
	  
  }
  
  public String listCountryLanguages(int cid){
	  String result = "";
	  
	  try {
		  
		  ps = connection.prepareStatement(
				  "SELECT lid, lname, population*(lpercentage) as population" +
				  " FROM country JOIN language ON country.cid = language.cid" +
				  " WHERE country.cid = ?" +
				  " ORDER BY population");
		  
		  ps.setInt(1, cid);
		 
		  rs = ps.executeQuery();
		 

		  while (rs.next()) {
			  result += String.valueOf(rs.getInt(1)) + ":" +
					  	rs.getString(2) + ":";
			  
			  if (rs.isLast()) {
				  result += String.valueOf(rs.getFloat(3));
			  }
			  else {
				  result += String.valueOf(rs.getFloat(3)) + "#";
			  }
		  }
		  
		  rs.close();
		  ps.close();
		  
	  }
		  
	  catch (SQLException e) {
		close();
	  }
		  
	return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
			ps=connection.prepareStatement("SELECT height FROM country WHERE cid=?");
			ps.setInt(1,cid);
			int diff=0;
			rs=ps.executeQuery();
			while (rs.next()){
				diff=rs.getInt("height");
			}
			ps.close();
			rs.close();
			
			int putmein=diff-decrH;
			
		  ps = connection.prepareStatement(
				  "UPDATE country SET height = ? WHERE cid = ?");
		  
		  ps.setInt(1, putmein);
		  ps.setInt(2, cid);
		  
		  if (ps.executeUpdate() == 0) {
			 
			  return false;
		  }
		  
		  ps.close();
		  
	  }
	  catch (SQLException e) {
	      close();
		  return false;
		  
	  }
		  
	  return true;
  }
    
  public boolean updateDB(){
	  try {
		  
		  ps = connection.prepareStatement(
				  "DROP TABLE IF EXISTS mostPopulousCountries");
		  
		  ps.executeUpdate();
		  
		  ps.close();

	  }

	  catch (SQLException e){
			
	  }

	  try {
		 
		  ps = connection.prepareStatement(
				  "CREATE TABLE mostPopulousCountries (" +
				  "		cid 		INTEGER," +
				  "		cname		VARCHAR(20)," +
				  "		PRIMARY KEY(cid))");
		  
		  ps.executeUpdate();
		  
		  ps.close();
		 

		  ps = connection.prepareStatement(
				  "SELECT cid, cname" +
				  " FROM country" +
				  " WHERE population > 100000000" +
				  " ORDER BY cid ASC");
		  
		  rs = ps.executeQuery();
		  
		  PreparedStatement ps2 = ps;
		  ps = connection.prepareStatement(
				  "INSERT INTO mostPopulousCountries(cid, cname)" +
				  " VALUES(?, ?)");
		  
		  while(rs.next()) {
			  ps.setInt(1, rs.getInt(1));
			  ps.setString(2, rs.getString(2));
			  
			  if (ps.executeUpdate() == 0) {
				  close();
				  return false;
			  }
		  }
		  
		  ps2.close();
		  rs.close();
		  ps.close();
		  
		  
	  }
	  catch (SQLException e){
		  close();
		  return false;
		  
	  }
	  
	  return true;
  }
}
