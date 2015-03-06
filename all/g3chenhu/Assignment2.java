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

  // sqltext for convenience
  String sqlText;
  
  //CONSTRUCTOR
  Assignment2(){
    try{
      Class.forName("org.postgresql.Driver");
    }catch(ClassNotFoundException e){}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try{
      connection = DriverManager.getConnection(URL, username, password);
      }catch(SQLException e){}

    if(connection != null){
      return true;
    }
    return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try{
      if(connection != null){
        connection.close();
      }}catch(SQLException e){}
    return true;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try{
      sqlText="SELECT * From a2.country WHERE cid = ?";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      rs=ps.executeQuery();
      if(rs.next()){
        rs.close();
        ps.close();
        return false;
      }else{
        sqlText="INSERT INTO a2.country VALUES(?,?,?,?)";
        ps=connection.prepareStatement(sqlText);
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);
        ps.executeUpdate();
        ps.close();
        return true;
      }
    }catch(SQLException e){}
    return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	 try{
    sqlText="SELECT COUNT(cid) FROM a2.oceanAccess WHERE oid = ?";
    ps=connection.prepareStatement(sqlText);
    ps.setInt(1, oid);
    rs=ps.executeQuery();
    rs.next();
    int result = rs.getInt("count");
    rs.close();
    ps.close();
    return result;
   }catch(SQLException e){}
   return -1;
  }
   
  public String getOceanInfo(int oid){
    try{
      sqlText="SELECT * From a2.ocean WHERE oid = ?";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs=ps.executeQuery();
      if(!rs.next()){
        rs.close();
        ps.close();
        return "";
      }else{
        String result=rs.getInt("oid")+":"+rs.getString("oname")+":"+rs.getInt("depth");
        rs.close();
        ps.close();
        return result;
      }
    }catch(SQLException e){}
    return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try{
      sqlText="UPDATE a2.hdi SET hdi_score=? WHERE cid=? And year=?";
      ps=connection.prepareStatement(sqlText);
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      int result=ps.executeUpdate();
      ps.close();
      if (result==1){
        return true;
      }
      }catch(SQLException e){}
    return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try{
      sqlText="DELETE FROM a2.neighbour WHERE country=? and neighbor=?";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      int result1=ps.executeUpdate();
      ps.setInt(1, c2id);
      ps.setInt(2, c1id);
      int result2=ps.executeUpdate();
      ps.close();
      if(result1==1 && result2==1){
        return true;
      }
    }catch(SQLException e){}
    return false;        
  }
  
  public String listCountryLanguages(int cid){
    try{
      sqlText="SELECT l.lid lid, l.lname lname, c.population*l.lpercentage population "+
              "FROM a2.country c join a2.language l on c.cid=l.cid "+
              "WHERE c.cid = ?";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1,cid);
      rs=ps.executeQuery();
      if(!rs.next()){
        rs.close();
        ps.close();
        return "";
      }else{
        String result = rs.getInt("lid") + ":" + rs.getString("lname") + ":"
                   +  rs.getFloat("population");
        while(rs.next()){
          result = result + "#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":"
                   +  rs.getFloat("population");
        }
        rs.close();
        ps.close();
        return result;
      }
    }catch(SQLException e){}
	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    try{
      sqlText="UPDATE a2.country SET height=height-? WHERE cid=?";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1,decrH);
      ps.setInt(2,cid);
      int result = ps.executeUpdate();
      ps.close();
      if(result==1){
        return true;
      }
    }catch(SQLException e){}
    return false;
  }
    
  public boolean updateDB(){
    try{
      sqlText="CREATE TABLE mostPopulousCountries(cid int, cname varchar(20))";
      ps=connection.prepareStatement(sqlText);
      int result = ps.executeUpdate();
      if (result!=1){
        return false;
      }
      sqlText="CREATE INDEX cidt ON mostPopulousCountries(cid ASC)";
      ps=connection.prepareStatement(sqlText);
      result = ps.executeUpdate();

      sqlText="SELECT c.cid cid, c.cname cname from country where population>=100000000";
      ps=connection.prepareStatement(sqlText);
      rs = ps.executeQuery();
      if(!rs.next()){
        System.out.println("false");
        return false;
      }

      sqlText="INSERT INTO mostPopulousCountries VALUES(?,?)";
      ps=connection.prepareStatement(sqlText);
      ps.setInt(1, rs.getInt("cid"));
      ps.setString(2, rs.getString("cname"));
      result = ps.executeUpdate();
      if (result!=1){
        return false;
      }

      while(rs.next()){
        ps=connection.prepareStatement(sqlText);
        ps.setInt(1, rs.getInt("cid"));
        ps.setString(2, rs.getString("cname"));
        result = ps.executeUpdate();
        if (result!=1){
          return false;
        }
      }
      return true;
 
    }catch(SQLException e){e.printStackTrace();}
	return true;    
  }

  public static void main(String[] args){}
}
