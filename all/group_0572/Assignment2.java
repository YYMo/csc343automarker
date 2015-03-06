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
		  System.out.println("Failed to find the JDBC driver");
		  return;
	  }
	  System.out.println("Found Driver!");
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
		  if (connection != null) {
			  return true;
		  }
      }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
		  
	  }
	  return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try {
		  connection.close();
		  return true;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return false;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try{
		  ps = 
			  connection.prepareStatement("insert into a2.country (cid, name, height, population)" +
			  " values(?, ?, ?, ?");
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  ps.executeUpdate();
		  
		  while (rs.next()) {
			  if (rs.getInt("cid") == cid) {
				  return false;
			  }
		  }
		  ps.close();
		  return true;
		  }
	  
	  catch (SQLException se){
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return false;
}
  
  public int getCountriesNextToOceanCount(int oid) {
	  try {
		  ps = connection.prepareStatement("select count(cid) from a2.oceanAccess where oid =" + oid);
		  rs = ps.executeQuery();
		  int count = -1;
		  while (rs.next()) {
			  count = rs.getInt("count");
		  }
		  
		  return count;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return -1;
  }
   
  public String getOceanInfo(int oid){
	  try {
		  ps = connection.prepareStatement("select oid, oname, depth from a2.ocean where oid =" + oid);
		  rs = ps.executeQuery();
		  int newOid = 0;
		  String name = "";
		  int depth = 0;
		  while (rs.next()) {
			  newOid = rs.getInt("oid");
			  name = rs.getString("oname");
			  depth = rs.getInt("depth");
		  }
		  return  newOid + ":" + name + ":" + depth;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		  ps = connection.prepareStatement("update a2.hdi set hdi_score =" + newHDI +
		  		" where cid = " + cid + " and year = " + year);
		  ps.executeUpdate();
		  if (ps != null) {
			  ps.close();
			  return true;
		  }
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		  ps = connection.prepareStatement("select * from a2.neighbour");
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  if (((c1id == rs.getInt("country")) && (c2id == rs.getInt("neighbor"))) || 
					  ((c2id == rs.getInt("country")) && (c1id == rs.getInt("neighbor")))) {
				  rs.deleteRow();
			  }
		  }
		  ps.close();
		  return true;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }      
	  return false;
  }
  
  public String listCountryLanguages(int cid){
	  try {
		  ps = connection.prepareStatement("select l.cid, lid, lname, (lpercentage * population) as pop " +
		  		" from a2.language l, a2.country c where l.cid =" + cid + " and c.cid =" + cid);
		  rs = ps.executeQuery();
		  
		  String answer = "";
		  while (rs.next()) {
			  answer = answer + rs.getInt("lid") 
			  	+ ":" + rs.getString("lname") + ":" + rs.getFloat("pop") + "#";
		  }
		  return answer;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
		  ps = connection.prepareStatement("update a2.country set height= height -" 
				  + decrH + " where cid=" + cid);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return false;
  }
    
  public boolean updateDB(){
	  try {
		  sql = connection.createStatement();
		  sql.executeUpdate("create table mostPopulousCountries (" +
			  		"cid INTEGER NOT NULL, cname VARCHAR(20) NOT NULL, PRIMARY KEY (cid))");
		  
		  String insert = "insert into mostPopulousCountries (select cid, cname from a2.country" +
		  		"where population > 100000000 order by cid asc)";
		  sql.executeUpdate(insert);
		  if (sql != null) {
			  sql.close();
			  return true;
		  }
	  }
	  catch (SQLException se) {
		  System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  }
	  return false;
  }

//  public static void main(String[] args) {
//	Assignment2 test = new Assignment2();
//	test.connectDB("jdbc:postgresql://localhost:5432/csc343h-c2chuajf", "c2chuajf", "");
//	//System.out.println("Connected: " + connection);
//	test.getOceanInfo(1);
//	//test.insertCountry(14, "Bobington", 6, 7);
//	test.getCountriesNextToOceanCount(1);
//	test.chgHDI(1, 2009, 3.9f);
//	test.deleteNeighbour(11,12);
//	test.listCountryLanguages(2);
//	// test.updateHeight(7,1);
//	test.updateDB();
//  }
}