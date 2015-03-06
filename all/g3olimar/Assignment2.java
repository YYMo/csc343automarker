import java.sql.*;

public class Assignment2 {
  
    /*public static void main (String args[]){
        Assignment2 a = new Assignment2();
        boolean e = a.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3olimar","g3olimar","");
        e= a.insertCountry(11,"Nigeria",101,1000000);
        System.out.println(a.getCountriesNextToOceanCount(1));
        System.out.println(a.getOceanInfo(1));
	e = a.chgHDI(3, 2009, 100);
	e = a.deleteNeighbour(3, 4);
	System.out.println(a.listCountryLanguages(2));
	e = a.updateDB();
        e = a.disconnectDB();
	
    }*/
    
    
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
          //System.out.println("Failed to find the JDBC driver");
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try
      {
          connection = DriverManager.getConnection(URL, username, password);
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try
      {
          rs.close();
          ps.close();
          connection.close();
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true;   
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   try
      {
          ps = connection.prepareStatement("INSERT INTO a2.country (cid, cname, HEIGHT, population)VALUES (?, ?, ?, ?)");
          ps.setInt(1,cid);
          ps.setString(2,name);
          ps.setInt(3,height);
          ps.setInt(4,population);
          
          ps.executeUpdate();
          //System.out.println("Record has been inserted.");
          
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true; 
  }
  
  public int getCountriesNextToOceanCount(int oid) {
        
        try
        {
          String q = "SELECT COUNT(*) AS num FROM a2.oceanAccess WHERE oid = ?";
            ps = connection.prepareStatement(q);
          ps.setInt(1,oid);
          
          rs = ps.executeQuery();
	  rs.next();
          return rs.getInt("num");
          
        }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return -1;
        } 
  }
   
  public String getOceanInfo(int oid){
	try
        {
          String q = "SELECT oid,oname,depth FROM a2.ocean WHERE oid = ?";
            ps = connection.prepareStatement(q);
          ps.setInt(1,oid);
          String ret;
          rs = ps.executeQuery();
	  while(rs.next()){
          	ret = rs.getInt("oid") +":"+ rs.getString("oname") +":"+ rs.getInt("depth");
		return ret;
          }
        }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return "";
        } 
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   try
      {
          ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and year = ?");
          ps.setFloat(1,newHDI);
          ps.setInt(2,cid);
          ps.setInt(3,year);
          
          ps.executeUpdate();
          //System.out.println("Record has been updated.");
          
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true; 
  
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try
      {
          ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?)");
          ps.setInt(1,c1id);
          ps.setInt(2,c2id);
          ps.setInt(3,c2id);
          ps.setInt(4,c1id);
          
          ps.executeUpdate();
          //System.out.println("Records have been deleted.");
          
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true;         
  }
  
  public String listCountryLanguages(int cid){
	String ret = "";
	try{
		String q = "SELECT lid,lname,(lpercentage*population/100) AS lpopulation FROM a2.language l JOIN a2.country c ON l.cid = c.cid WHERE c.cid = ?";
		ps = connection.prepareStatement(q);
		ps.setInt(1,cid);
		
		rs = ps.executeQuery();
		while(rs.next()){
			  ret = ret + rs.getInt("lid") +":"+ rs.getString("lname") +":"+ rs.getInt("lpopulation") +"#";
		}
		return ret;
        }catch (SQLException se) {
		  //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
		  return "";
        }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try
      {
          ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?");
          ps.setInt(1,decrH);
          ps.setInt(2,cid);
          
          ps.executeUpdate();
          //System.out.println("Record has been updated.");
          
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true; 
  }
    	

  public boolean updateDB(){
	try
      {
	sql = connection.createStatement();
	String sqlTxt = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE";
	sql.executeUpdate(sqlTxt);
	sqlTxt = "CREATE TABLE a2.mostPopulousCountries ("+
	    "cid 		int,"+
	    "cname 		varchar(20)"+
	    "						)";
	sql.executeUpdate(sqlTxt);
        sqlTxt = "INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100000000)";
	sql.executeUpdate(sqlTxt);
          //System.out.println("Table has been updated.");
          
      }catch (SQLException se) {
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          return false;
      }
      return true;     
  }
  
}

