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
        catch (Exception e) {
            e.printStackTrace();
          }

  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
     
      try {
            connection = DriverManager.getConnection(URL, username, password);
      }
      catch (SQLException e) {
         e.printStackTrace();
      }

      if (connection != null) {
        return true;
      }
      else {
        return false;
      }
   
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
      ps.close();
      rs.close();

    }
     catch (Exception e) {
      return false;   
     }

    return true;
       
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    //Execute the following query:
    //Select * from a2.country c where c.cid = cid
    //if above returns a value, then return false
    //otherwise, insert value into a2.country
    
      try {
        int count = 0;
    String queryString = "select * from a2.country c where c.cid = ?";
    ps = connection.prepareStatement(queryString);
    ps.setInt(1, cid);
    rs = ps.executeQuery();

    while (rs.next()) {
      count += 1;
      break;
    }
    if (count == 1) {
      return false;

    }
      
    else {
         queryString = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
         ps = connection.prepareStatement(queryString);
         ps.setInt(1, cid);
         ps.setString(2, name);
         ps.setInt(3, height);
         ps.setInt(4, population);
         int val = ps.executeUpdate();


         
         if (val == 0)
            return true;
         else 
            return false;

    }

       
      }
      catch (Exception e) {
        e.printStackTrace();
       
      }

      return false;


   
  }
  
  public int getCountriesNextToOceanCount(int oid) {

     PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;

    try {
      String queryString = "select count(cid) from a2.oceanaccess where oid = ?";
    int val = 0;
    ps = connection.prepareStatement(queryString);
    ps.setInt(1, oid);
    rs = ps.executeQuery();
    while (rs.next()) {
      val = ((Number) rs.getObject(1)).intValue();
    }
    return val;

    }
    catch (Exception e) {
      e.printStackTrace();
      return -1;
    }
    
	
  }
   
  public String getOceanInfo(int oid){

    PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;

  int count = 0;
    String returnString = "";

    


    try {

    String queryString = "select * from a2.ocean o where o.oid = ?";
    ps = connection.prepareStatement(queryString);
    ps.setInt(1, oid);
    rs = ps.executeQuery();
   

    while (rs.next()) {
       int oid_col = rs.getInt("oid");
    String oname = rs.getString("oname");
    String depth = rs.getString("depth");
    //System.out.println(oid_col+":"+oname+":"+depth);
    returnString += oid_col+":"+oname+":"+depth;


      count = 1;
      break;
    }

    return returnString;

    /*if (count == 1) {

      //returnString.changeTo(oid_col+":"+oname+":"+depth);
      System.out.println(oid_col+":"+oname+":"+depth);

    } */

    }
    catch (Exception e) {
      e.printStackTrace();

    }

    
   return returnString;
  }

  public boolean chgHDI(int cid, int year, float newHDI){


    PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;

    String queryString = "update a2.hdi set hdi_score = ? where cid = ? and year = ?";

    try {
       ps = connection.prepareStatement(queryString);
    ps.setFloat(1, newHDI);
    ps.setInt(2, cid);
    ps.setInt(3, year);


    int val = ps.executeUpdate();


         
         if (val == 0)
            return false;
         else 
            return true;

    }
    catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    

  
  }

  public boolean deleteNeighbour(int c1id, int c2id){

    PreparedStatement ps1;
  
  // Resultset for the query
  ResultSet rs1;

  PreparedStatement ps2;
  
  // Resultset for the query
  ResultSet rs2;


    String queryString = "delete from a2.neighbour where country = ? and neighbor = ?";
    //String queryString2 = "delete from a2.neighbour where country = ? and neighbor = ?";



    try {
      ps1 = connection.prepareStatement(queryString);
         ps1.setInt(1, c1id);
         ps1.setInt(2, c2id);
         
         int val1 = ps1.executeUpdate();
    ps2 = connection.prepareStatement(queryString);
      ps2.setInt(1, c2id);
      ps2.setInt(2, c1id);

      int val2 = ps2.executeUpdate();

      if ((val1 + val2) == 2)
        return true;
      else
        return false;

    }

    catch (Exception e) {
      e.printStackTrace();
      return false;
    }
          



   
  }
  
  public String listCountryLanguages(int cid){

    PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;


    String queryString = " select l.lid, lname, coalesce(lpercentage,0) * coalesce(population, 0) as population " + 
"from a2.language l join a2.country c on l.cid = c.cid where l.cid = ?";


    String returnString = "";

    try {

       ps = connection.prepareStatement(queryString);
    ps.setInt(1, cid);
    rs = ps.executeQuery();
   

    while (rs.next()) {
       int lid = rs.getInt("lid");
    String lname = rs.getString("lname");
    float population = rs.getFloat("population");
    //System.out.println(oid_col+":"+oname+":"+depth);
    if (rs.isLast())
       returnString += lid+":"+lname+":"+population;
        
    else {
          returnString += lid+":"+lname+":"+population+"#";


    }
  
     

    
 
    }
    return returnString;

    }
    catch (Exception e) {
      e.printStackTrace();
      return "";
    }
   

    
   

	
  }
  
  public boolean updateHeight(int cid, int decrH){

     PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
    String queryString = "update a2.country set height = ? where cid = ?";



    try {
      ps = connection.prepareStatement(queryString);
         ps.setInt(1, decrH);
         
         ps.setInt(2, cid);
         int val = ps.executeUpdate();

         if (val == 1)
            return true;
         else 
            return false;

    }
    catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    
  }
    
  public boolean updateDB(){
	return false;    
  }

  /*public  static void main(String args[]) {
  Assignment2 x = new Assignment2();
  System.out.println(x.connectDB("jdbc:postgresql://localhost:5432/csc343h-c4bhanda", "c4bhanda", ""));
  x.insertCountry(1, "Canada", 500, 35000000);
  System.out.println(x.getCountriesNextToOceanCount(1));
  System.out.println(x.getOceanInfo(1));
  System.out.println(x.chgHDI(4, 2010, (float) 1.0));
  System.out.println(x.deleteNeighbour(4, 5));
  System.out.println(x.updateHeight(1, 666));
  System.out.println(x.listCountryLanguages(4)); 



 }  */
  
}

 