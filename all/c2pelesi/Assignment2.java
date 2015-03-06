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
		  Class.forName("org.postgreSQL.Driver");
	  }
	  	  catch(ClassNotFoundException ex){		  
	  }
  }
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try{
		  connection = DriverManager.getConnection(URL, username, password);
	  }
	  catch(SQLException ex){
		  return false;
	  }
	  return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try{
		  connection.close();
	  }
      catch(SQLException ex){
    	  return false;    
      }
	  return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      int dmlOutputLength;   
	  try{
		  ps = connection.prepareStatement("INSERT INTO a2.country "
		  									+ "VALUES(?,?,?,?)");
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  System.out.println(dmlOutputLength = ps.executeUpdate());
		  ps.close();
		  if(dmlOutputLength != 1)
          {
              return false;
		  }
          return true;
	  }
	  catch(SQLException ex){
		  return false;
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int num;
	  try{
		  ps = connection.prepareStatement("SELECT COUNT(DISTINCT cid)"
				  + "FROM a2.oceanAccess WHERE oid = ?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  rs.next();
		  num = rs.getInt(1);
		  ps.close();
		  rs.close();
	  }
	  catch(SQLException ex){
	  return -1;  
	  }
	  return num;
  }
   
  public String getOceanInfo(int oid){
	  String oceanInfo;
	  try{
		  ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  rs.next();
		  oceanInfo = Integer.toString(oid) + ":" + rs.getString(2) + 
				  		   ":" + Integer.toString((rs.getInt(3)));
		  ps.close();
		  rs.close();
	  }
	  catch(SQLException ex){
		  return "";
	  }
	  return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try{
		  ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ?"
		  		+ " WHERE cid = ? AND year = ?");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  }
	  catch(SQLException ex){
		  return false;
	  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
		  ps = connection.prepareStatement("DELETE FROM a2.neighbour"
		  									+ " WHERE (country = ? AND neighbor = ?)"
		  									+ " OR (neighbor = ? AND country = ?)");
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
		  ps.setInt(3, c1id);
		  ps.setInt(4, c2id);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  }
	  catch(SQLException ex){
		  return false;
	  }
        
  }
  
  public String listCountryLanguages(int cid){
	  StringBuilder langList = new StringBuilder("");
	  try
	  {
		  ps = connection.prepareStatement(   "SELECT lid, lname, country.population*lpercentage"
		  									+ " AS populationx FROM a2.language, a2.country "
		  									+ " WHERE country.cid = language.cid AND country.cid = ?"
		  									+ " ORDER BY populationx");
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  while(rs.next())
			  {  
				  langList.append("#" + Integer.toString(rs.getInt(1)) + ":" + rs.getString(2) + ":"
					  		  	   + Float.toString(rs.getFloat(3)));
			  }
		  langList.deleteCharAt(0);
		  ps.close();
		  rs.close();
		  return langList.toString();
	  }
	  catch(SQLException ex){
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try
	  {
		  ps = connection.prepareStatement("UPDATE a2.country SET height = height - ?"
		  									+ "WHERE cid = ?");
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  ps.executeUpdate();
		  ps.close();
		  return true;
		  
	  }
	  catch(SQLException ex)
	  {
		  return false;
	  }
  }
    
  public boolean updateDB(){
	  try
	  {
		  //Create the table
		  ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries ("
		  								    + " cid INTEGER, cname VARCHAR(20))");
		  ps.executeUpdate();
		  ps.close();
		  //Create a new prepared statement to populate it
		  ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries "
				    						+ "(SELECT cid, cname"
				    						+ " FROM a2.country"
				    						+ " WHERE population > 100000000"
				    						+ " ORDER BY cid ASC)");
		  ps.executeUpdate();
		  ps.close();
		  return true;
		  
	  }
	  catch(SQLException ex)
	  {
		  return false;  
	  }    
  }
  
}
