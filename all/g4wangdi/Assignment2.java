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
  public Assignment2(){
      try{
          Class.forName("org.postgresql.Driver");
      }catch(ClassNotFoundException e){
          System.out.println(e);
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
        connection = DriverManager.getConnection( URL, username, password );
        return true;
      } catch (SQLException err){
          return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
        connection.close();
        return true;
      }catch (SQLException err){
          return false;
      }   
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try{
        sql = connection.createStatement();
        sql.executeUpdate("INSERT INTO country(cid, cname, height, population) "+"VALUES ("+Integer.toString(cid)+", " +name+", "+Integer.toString(height)+", "+Integer.toString(population)+")");
        sql.close();
        return true;
      } catch (SQLException err){
          return false;
      }
      
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      try{
        sql = connection.createStatement();
        rs = sql.executeQuery("select count(cid) from oceanAccess where oid="+Integer.toString(oid));
        int val = 0;
        while(rs.next()){
	  val =  ((Number) rs.getObject(1)).intValue();
        }
        rs.close();
        sql.close();
        return val;
      } catch (SQLException err){
          return -1;
      } 
  }
   
  public String getOceanInfo(int oid){
      try{
        String s = "";
        sql = connection.createStatement();
        rs = sql.executeQuery("select oname,depth from ocean where oid="+Integer.toString(oid));
        while (rs.next()){
	  s += Integer.toString(oid)+":" + rs.getObject(1).toString();
	  s += ":" + rs.getObject(2).toString();
        }
        sql.close();
        rs.close();
        return s;
      }catch (SQLException err){
          return "";
      }
      
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try{
          sql = connection.createStatement();
          sql.executeUpdate("UPDATE hdi SET hdi_score = " + String.valueOf(newHDI) 
                  +"where cid = " + Integer.toString(cid) +"and year = " +Integer.toString(year));
          sql.close();
          return true;
      }catch (SQLException err){
          return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try{
          sql = connection.createStatement();
          sql.executeUpdate("DELETE FROM neighbour" + " where country = " + Integer.toString(c1id) + " and " 
                  + "neighbor = " + Integer.toString(c2id));
          sql.executeUpdate("DELETE FROM neighbour" + " where neighbor = " + Integer.toString(c1id) + " and " 
                  + "country = " + Integer.toString(c2id));
          sql.close();
          return true;
         }catch (SQLException err){
          return false;
         }
  }
  
  public String listCountryLanguages(int cid){
	try{
            sql = connection.createStatement();
            String s="";
            rs = sql.executeQuery("select lid, lname, population*lpercentage from country natural join language where cid = " + Integer.toString(cid));
            while (rs.next()){
               String lid = Integer.toString(rs.getInt(1));
               String lname = rs.getString(2);
               String lp = String.valueOf(rs.getObject(3));
               s += lid+":"+lname+":"+lp+"#";
            }
            return s.substring(0,s.length()-1);
        }catch (SQLException err){
          return "";
         }
  }
  
  public boolean updateHeight(int cid, int decrH){
        try{
            sql = connection.createStatement();
            sql.executeUpdate("UPDATE country SET height = height - " + String.valueOf(decrH) 
                  +"where cid = " + Integer.toString(cid));
            sql.close();
            return true;
        }catch (SQLException err){
            return false;
        }
  }
    
  public boolean updateDB(){
	try{
	   Statement sql1 = connection.createStatement();
	   sql = connection.createStatement();
	   sql.executeUpdate("CREATE TABLE mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL)");
            rs=sql.executeQuery("select cid,cname from country where population > 100000000order by cid ASC");
            int i = 0;
            String s = "";
            while (rs.next()){
		i = rs.getInt(1);
		s = rs.getString(2);
		sql1.executeUpdate("INSERT INTO mostPopulousCountries values ("+ Integer.toString(i)+",'"+s+"')");
            }
            rs.close();
            sql.close();
            sql1.close();
            return true;
        } catch (SQLException err){
            return false;
        }
  }
  
}
