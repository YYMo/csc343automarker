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
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try{
      connection = DriverManager.getConnection(URL, username, password);
    }catch (SQLException e){
      System.err.println("SQException: " + e.getMessage());
      return false;
    }  
    return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
        connection.close();
      }catch(SQLException ex){
        System.err.println("SQException: " + ex.getMessage());
        return false;
      }
      return true;
  }
    
  public boolean insertCountry (int cid, String cname, int height, int population) {
      try{
        String sqlText = "INSERT INTO a2.country VALUES (?,?,?,?)";
        ps = connection.prepareStatement(sqlText);
        ps.setInt(1,cid);
        ps.setString(2,cname);
        ps.setInt(3,height);
        ps.setInt(4, population);
        ps.executeUpdate();
        ps.close();
        connection.close();
       }catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage());  
         return false; 
     }
     return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    int count = 0; 
      try{
        String country = "SELECT COUNT(a2.oceanAccess.cid) FROM a2.oceanAccess where a2.oceanAccess.oid = " + oid;
        sql = connection.createStatement(); 
        rs = sql.executeQuery(country);
        if(rs.next()){
          count = rs.getInt(1);
        }
        sql.close();
        rs.close();

      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage());
         return -1;  
     }
     return count;
  }

  public String getOceanInfo(int oid){
   int count = 0; 
   int check = 0;
   //String ocean;
   String empty = "";
   
      try{
        String country = "SELECT a2.ocean.oid, a2.ocean.oname, a2.ocean.depth FROM a2.ocean where a2.ocean.oid = " + oid;

        sql = connection.createStatement(); 

        rs = sql.executeQuery(country);
        if(rs.next()){
          String ocean = rs.getInt(1) + ":"+rs.getString(2) + ":"+rs.getInt(3);
          check = 1;
          sql.close();
          rs.close();
          if (check == 1){
            return ocean;
          }
        }
      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage()); 
     }
   return empty ;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try{
        String query = "UPDATE a2. hdi SET hdi_score = ? WHERE a2.hdi.cid = ? and a2.hdi.year = ?";
        ps = connection.prepareStatement(query);
        ps.setInt(2,cid);
        ps.setFloat(3,year);
        ps.executeUpdate();
        ps.close();
        connection.close();

      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage()); 
         return false;
     }

   return true ;
  }
   
    public boolean deleteNeighbour(int c1id, int c2id){
      try{
        String query = "DELETE from a2.neighbour Where (a2.neighbour.country = ? and a2.neighbour.neighbor = ?) or (a2.neighbour.country = ? and a2.neighbour.neighbor = ?)";
        ps = connection.prepareStatement(query);
        ps.setInt(1,c1id);
        ps.setInt(2,c2id);
        ps.setInt(3,c2id);
        ps.setInt(4,c1id);
        ps.executeUpdate();
        ps.close();
        connection.close();

      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage()); 
         return false;
     }

   return true ;       
  }

 public String listCountryLanguages(int cid){
  String empty = "";
  String sBuffer = new String("");
      try{
        String query = "SELECT a2.language.lid, a2.language.lname, (a2.language.lpercentage * a2.country.population) as population FROM a2.country  , a2.language  WHERE a2.country.cid = a2.language.cid and a2.country.cid ="+cid+"  ORDER BY population;";
        sql = connection.createStatement(); 
        rs = sql.executeQuery(query);
      ;
      
        while(rs.next()){

          sBuffer=  sBuffer+rs.getInt(1) + ":"+rs.getString(2) + ":"+rs.getInt(3) + "#" ;
        }
        sql.close();
        rs.close();
        
        return sBuffer.substring(0,sBuffer.length()-1);

      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage()); 
     }

   return empty ;   
 }

  public boolean updateHeight(int cid, int decrH){
      try{
        String query = "UPDATE a2.country SET height = (height - ?) WHERE a2.country.cid = ?";
        ps = connection.prepareStatement(query);
        ps.setInt(1,decrH);
        ps.setInt(2,cid);
        ps.executeUpdate();
        ps.close();
        connection.close();

      } catch (SQLException e)
     {
         System.err.println("SQException: " + e.getMessage()); 
         return false;
     }

   return true ;       
  }

  public boolean updateDB(){
    try{
      String allInsert = "";
      String cmd = "";
      sql = connection.createStatement();
      String table = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT,cname VARCHAR(20) NOT NULL);";
      String over = "SELECT cid, cname FROM a2.country WHERE population > 100000000";
      rs = sql.executeQuery(over);
      while(rs.next()){
        int ocid = rs.getInt("cid");
        String ocname = rs.getString("cname");
        allInsert = allInsert + "INSERT INTO a2.mostPopulousCountries VALUES("+ocid+", '"+ocname+"'); ";
      }
      cmd = table + allInsert;
      sql.executeUpdate(cmd);
    } catch (SQLException e) {
        System.err.println("SQLException: " + e.getMessage());
        return false;
      }
    return true;
  }
  

  // public static void main(String[] args){
  //   Assignment2_skeleton ass = new Assignment2_skeleton();
  //   boolean a, b, c;
  //   int d;
  //   String e;
  //   float f = 0.3f;
  //   a = ass.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3helios", "g3helios", "\0");
  //   b = ass.updateDB();
  //   c = ass.disconnectDB();
 
  // }
  
}
