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
	} catch (ClassNotFoundException e) {
		System.err.println(e);
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
		  sql = connection.createStatement();
		  ps = connection.prepareStatement("SET SEARCH_PATH TO a2");
		  ps.executeUpdate();
		  return true;
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try {
		  sql.close();
		  rs.close();
		  ps.close();
		  connection.close();
		  return true;
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		  //Check if integer arguments are positive integers.
		  if (cid < 0 || height < 0 || population < 0) {
			  return false;
		  }
		  String sqlquery = "INSERT INTO a2.country " + 
		                    "VALUES (" + cid + ", '" + name + "', " + height + ", " + population + ")";
		  sql.executeUpdate(sqlquery);
		  return true;
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try {
		  //check if oid is positive integer.
		  if (oid < 0) {
			  return -1;
		  }
		  String sqlquery = "SELECT COUNT(cid) " +
		                    "FROM a2.oceanAccess " +
				            "WHERE oid = " + oid;
		  rs = sql.executeQuery(sqlquery);
		  
		  //Since using COUNT aggregate function, only single tuple and single column(by statement)
		  rs.next();
		  int numCountries = rs.getInt(1);;
		  return numCountries;
	  } catch(SQLException e) {
		  System.err.println(e);
		  return -1;
	  }  
  }
   
  public String getOceanInfo(int oid){
	  try {
		  //Check if oid is positive integer.
		  if (oid < 0) {
			  return "";
		  }
		  String sqlquery = "SELECT * " +
		                    "FROM a2.ocean " +
		                    "WHERE oid = " + oid;
		  rs = sql.executeQuery(sqlquery);
		  String oceaninfo = "";
		  ResultSetMetaData rsmd = rs.getMetaData();
		  int numcol = rsmd.getColumnCount();
		  
		  //Since oid is primary key, only one tuple will be selected for single oid.
		  if (rs.next()) {
			  for (int i = 1; i < numcol; i++) {
			      oceaninfo += rs.getString(i) + ":";
		      }
		      oceaninfo += rs.getString(numcol);
		  }
		      
		  return oceaninfo;
		  
	  } catch(SQLException e) {
		  System.err.println(e);
		  return "";
	  }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		  //check if arguments are positive integer/float where 0<=newHDI<=1.
		  if (cid < 0 || year < 0 || newHDI < 0 || newHDI > 1) {
			  return false;
		  }
		  //check if rs is empty.
		  rs = sql.executeQuery("SELECT * FROM a2.hdi WHERE cid = " + cid + " AND year = " + year);
		  if (rs.next()) {
			  String sqlquery = "UPDATE a2.hdi " +
		                    "SET hdi_score = " + newHDI +
				            " WHERE cid = " + cid + " AND year = " + year;
			  sql.executeUpdate(sqlquery);
		      return true;
		  } else {
			  return false;
		  }
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		  //check if arguments are in right formats(integers).
		  if (c1id < 0 || c2id < 0) {
			  return false;
		  }
		  //Check if rs is empty.
		  rs = sql.executeQuery("SELECT * FROM a2.neighbour WHERE" + 
				                " (country = " + c1id + " AND neighbor = " + c2id + ") " +
		                        "OR (country = " + c2id + " AND neighbor = " + c1id + ")");
		  if (rs.next()) {
			  String sqlquery = "DELETE " +
		                    "FROM a2.neighbour " +
				            "WHERE (country = " + c1id + " AND neighbor = " + c2id + ") " +
		                    "OR (country = " + c2id + " AND neighbor = " + c1id + ")";
		      sql.executeUpdate(sqlquery);
		      return true;     
		  } else {
			  return false;
		  }      
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }      
  }
  
  public String listCountryLanguages(int cid){
	  try {
		  //check if cid is in right form.
		  if (cid < 0) {
			  return "";
		  }
		  String sqlquery = "SELECT l.lid, l.lname, (l.lpercentage * c.population) AS population " +
		                    "FROM a2.language l JOIN a2.country c ON l.cid = c.cid " +
		                    "WHERE l.cid = " + cid +
		                    " ORDER BY population";
		  rs = sql.executeQuery(sqlquery);
		  String counlanglist = "";
		  ResultSetMetaData rsmd = rs.getMetaData();
		  int numcol = rsmd.getColumnCount();
		  while (rs.next()) {
			  for (int i = 1; i < numcol; i++) {
				  counlanglist += rs.getString(i) + ":";
			  }
			  counlanglist += rs.getString(numcol) + "#";
		  }
		  if (counlanglist != "") {
			  counlanglist = counlanglist.substring(0, counlanglist.length() - 1);
		  }
		  return counlanglist;
		  
	  } catch(SQLException e) {
		  System.err.println(e);
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
		  //check if arguments are in right forms.
		  if (cid < 0 || decrH < 0) {
			  return false;
		  }
		  //check if rs is empty.
		  rs = sql.executeQuery("SELECT * FROM a2.country WHERE cid = " + cid);
		  if (rs.next()) {
			  String sqlquery = "UPDATE a2.country " +
		  		            "SET height = height - " + decrH +
		  		            "WHERE cid = " + cid;
			  sql.executeUpdate(sqlquery);
			  return true;
		  } else {
			  return false;
		  }
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }
  }
    
  public boolean updateDB(){
	  try {
		  String tablename = "a2.mostPopulousCountries";

		  String sqlquery = "SELECT cid, cname " +
		                    "FROM a2.country " +
		                    "WHERE population > 100000000 " +
		                    "ORDER BY cid";
		  rs = sql.executeQuery(sqlquery);
		  
		  String sqlinsert = "INSERT INTO " + tablename +
			                 " VALUES (?, ?)";
		  ps = connection.prepareStatement(sqlinsert);
		  
		  //check if rs is empty. If not, store first row into new table
		  //and continue until the end of rows from sqlquery.
		  if (rs.next()) {
			  String sqlcreate = "CREATE TABLE " + tablename + "(" +
		                         "cid INTEGER, " +
		                         "cname VARCHAR(20))";
		      sql.executeUpdate(sqlcreate);
			  
			  ps.setInt(1, rs.getInt(1));
		      ps.setString(2, rs.getString(2));
		      ps.executeUpdate();
		      
		      while (rs.next()) {
			      ps.setInt(1, rs.getInt(1));
			      ps.setString(2, rs.getString(2));
			      ps.executeUpdate();
		      }
		      return true;
		  } else {
			  return false;
		  }
	  } catch(SQLException e) {
		  System.err.println(e);
		  return false;
	  }  
  }
}
