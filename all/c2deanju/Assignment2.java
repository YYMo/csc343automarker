//Juliana Dean
//999635257

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
  
  String querystring;

    int checkint;
  
  
  //CONSTRUCTOR
  Assignment2(){
      try {
	  Class.forName("org.postgresql.Driver");
      }catch (ClassNotFoundException e) {

      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
    	  connection = DriverManager.getConnection(URL, username, password);
	  return true;
	  
      }
      catch (SQLException se){
    	  return false;
      }
      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try { 
	  connection.close();
	  if (connection.isClosed()){
	      return true;
	  }else{
	      return false;
	  }
      }catch (SQLException se){
  
      }     
      return false;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      querystring = "insert into a2.country (cid, cname, height, population)" +
"values (" + Integer.toString(cid) + "," + "'" + name + "'" + "," +
 Integer.toString(height) + "," + Integer.toString(population) + ")";
      try {
	  sql = connection.createStatement();
	  checkint = sql.executeUpdate(querystring);
	  if (checkint == 0){
	      return false;
	  }
	  return true;
      }catch (SQLException se){

      }
      return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      querystring = "select count(cid) as cidcount from a2.oceanAccess where oid = ?";
      try {
	  ps = connection.prepareStatement(querystring);
	  ps.setInt(1, oid);
	  rs = ps.executeQuery();
          rs.next();
          int num = rs.getInt("cidcount");
          rs.close();   
          return num;
      }catch (SQLException se){
	  return -1;  
      }
  }
   
  public String getOceanInfo(int oid){
      querystring = "select * from a2.ocean where oid = ?";
      try {
	  ps = connection.prepareStatement(querystring);
	  ps.setInt(1, oid);
	  rs = ps.executeQuery();
	  while(rs.next()){
	      int ocid = rs.getInt("oid");
	      String oname = rs.getString("oname");
	      String odepth = rs.getString("depth");
	      System.out.println("info is:" + ocid + ":" + oname + ":" + odepth);
	      rs.close();
	      return ocid + ":" + oname + ":" + odepth;
	   }
	   rs.close();
	   return "";
      }catch (SQLException se){
      }
      return "";
  }
  

  public boolean chgHDI(int cid, int year, float newHDI){
      querystring = "update a2.hdi set hdi_score =" + Float.toString(newHDI) 
	  + " where cid =" 
	  +Integer.toString(cid)+ " and year =" + Integer.toString(year);
      try {
	 sql = connection.createStatement();
	 checkint = sql.executeUpdate(querystring);
      if (checkint == 0){
	  return false;
      }
	 return true;
      }catch (SQLException se){

      }
      return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      querystring = "delete from a2.neighbour where country =" + Integer.toString(c1id) + " and neighbor =" + Integer.toString(c2id);
      try {
	  sql = connection.createStatement();
	  checkint = sql.executeUpdate(querystring);
       if (checkint == 0){
       }
       }catch (SQLException se){
	  return false;  
       }
      querystring = "delete from a2.neighbour where country =" + Integer.toString(c2id) + " and neighbor =" + Integer.toString(c1id);
      try {
	  sql = connection.createStatement();
	  checkint = sql.executeUpdate(querystring);
       if (checkint == 0){
	   return false;
       }
	  return true;
       }catch (SQLException se){
	  return false;  
       }
	  
  }
  
  public String listCountryLanguages(int cid){
	querystring = "select lid, country.cname as lname, language.lpercentage * country.population " +
			"as population from a2.language, a2.country where language.cid = ? and " +
			"language.cid = country.cid order by population";
	try {
	   ps = connection.prepareStatement(querystring);
	   ps.setInt(1, cid);
	   rs = ps.executeQuery();
	   String finalstring = "";
	   while(rs.next()){
		   String langname = rs.getString("lname");
		   int newlid = rs.getInt("lid");
		   int newpop = rs.getInt("population");
		   if (finalstring.length() == 0){
		       finalstring = Integer.toString(newlid) + ":" + langname + ":" + Integer.toString(newpop);

		   }else{
		       finalstring = finalstring + "#" + Integer.toString(newlid) + ":" + langname + ":" + Integer.toString(newpop);
		   }
	   }
	   rs.close();
	   return finalstring;
	}catch (SQLException se){
	    return "";  
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
      querystring = "update a2.country set height =" + Integer.toString(decrH) +" where cid =" + Integer.toString(cid);
      try {
	  sql = connection.createStatement();
	  checkint = sql.executeUpdate(querystring);
	  if (checkint == 0){
	      return false;
	  }
	       return true;
      }catch (SQLException se){
	
	  return false;  
	}
  }
    
  public boolean updateDB(){
      boolean checkup = false;
      querystring = "create table a2.mostPopulousCountries as select cid, cname " 
	    +"from a2.country where population > 10";  
    try {
	    sql = connection.createStatement();
	    sql.executeUpdate(querystring);
	    checkup = true;

    }catch (SQLException se){
	   
	}
    return checkup;
}

}