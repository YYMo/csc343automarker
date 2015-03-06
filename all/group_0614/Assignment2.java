import java.sql.*;
import java.text.*;
import java.io.*;

public class Assignment2 {
  //methods to finish:
  //updatedb

  // A connection to the database
  Connection connection;

  // Statement to run queries
  Statement sql;

  // Prepared Statement
  PreparedStatement ps;

  // Resultset for the query
  ResultSet rs;

  //instance variables
  DatabaseMetaData dbmd;

  String queryString;

  //CONSTRUCTOR
  Assignment2(){
    try{
        Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException ex){

    }
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(URL,
                                       username,
                                       password); //connect to the db
      dbmd = connection.getMetaData(); //get MetaData to confirm connection
      System.out.println("Connection to "+dbmd.getDatabaseProductName()+" "+
                         dbmd.getDatabaseProductVersion()+" successful.\n");
      return true;
    } catch (SQLException ex){
      System.out.println("couldnt connect to db");
      return false;
    }

  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
        connection.close();
        return true;
      } catch (SQLException ex){
        return false;
      }
  }

  public boolean insertCountry(int cid, String name, int height, int population) {
    try{
      //if country cid already exists, dont insert into the table and return false
      queryString = "select * from a2.country where cid = ?";
      //prep statment
      ps = connection.prepareStatement(queryString);
      //grab args
      ps.setInt(1, cid);
      //execute query
      rs = ps.executeQuery();

      //if any values exist, insert into the table
      if (!rs.next()){
        //insert

        String sqlText = "insert into a2.country (cid, cname, height, population) values (?, ?, ?, ?)";
        //prep statement
        ps = connection.prepareStatement(sqlText);
        //set args
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);

        //execute
        System.out.println("Executing this command: "+sqlText+"\n");

        if(ps.executeUpdate() == 0){
          return false;
        }
        return true;
      }
    } catch (SQLException ex){
        System.out.println("could not insert country value");
        return false;
    }
    return false;
  }

  public int getCountriesNextToOceanCount(int oid) {
    try{
      int ret_val = 0;
      //get all cids of countries with direct access to this ocean
      queryString = "select cid from a2.oceanAccess where oid = ?";
      //prep statement
      ps = connection.prepareStatement(queryString);
      //set args
      ps.setInt(1, oid);
      //execute
      System.out.println("Executing this command: "+queryString+"\n");
      rs = ps.executeQuery();
      if (rs.next()){
        ret_val = ret_val + 1;
      }
      return ret_val;
    } catch (SQLException ex){
      System.out.print("Something went wrong");
      return -1;
    }
  }

  public String getOceanInfo(int oid){
   String oceanInfo = "";
   try{
       sql = connection.createStatement(); //create a statement that we can use later
       rs = sql.executeQuery("SELECT oid, oname, depth FROM a2.ocean WHERE " + "oid =" + oid);
       if(rs.next()){
           oceanInfo += rs.getString("oid") + ":" + rs.getString("oname") + ":"
           + rs.getInt("depth");
       }
   }

   catch (SQLException e){
       return oceanInfo;
   }

   return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){

   String sqlText = "UPDATE a2.hdi SET hdi_score = "+newHDI+ "WHERE cid =" +cid+ "and year =" +year;
   try{
     sql = connection.createStatement(); //create a statement that we can use later
     if(sql.executeUpdate(sqlText) == 0){
       return false;
     }
     return true;
   }

   catch (SQLException e){
    e.printStackTrace();
    return false;
   }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try{
      //delete first value
      queryString = "delete from a2.neighbour where country = ? and neighbor = ?";
      //prep
      ps = connection.prepareStatement(queryString);
      //set args
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      //execute
      if(ps.executeUpdate() == 0){
        return false;
      }
      //delete second value
      String queryString2;
      queryString2 = "delete from a2.neighbour where country = ? and neighbor = ?";
      //prep
      ps = connection.prepareStatement(queryString2);
      //set args
      ps.setInt(1, c2id);
      ps.setInt(2, c1id);
      //execute
      if(ps.executeUpdate() == 0){
        return false;
      }
      return true;
    } catch (SQLException ex){
      System.out.println("could not delete neighbour");
      return false;
    }
  }

  public String listCountryLanguages(int cid){
	 String listCountryLanguages = "";
   try {
       queryString = "SELECT lid, lname, lpercentage FROM a2.language WHERE cid =" + cid;
       //prep statment
       ps = connection.prepareStatement(queryString);
       //execute query
       rs = ps.executeQuery();

        /*
       rs = sql.executeQuery("SELECT lid, lname, population FROM a2.language WHERE"  + "cid =" + cid);
       */
       while (rs.next()){
           listCountryLanguages += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("lpercentage");
       }

       rs.close();
       return listCountryLanguages;

   }

   catch (SQLException e){
       e.printStackTrace();
       return listCountryLanguages;
   }
  }

  public boolean updateHeight(int cid, int decrH){
    try{
      //delete first value
      queryString = "update a2.country set height=? where cid=?";
      //prep
      ps = connection.prepareStatement(queryString);
      //set args
      ps.setInt(1, decrH);
      ps.setInt(2, cid);
      //execute
      System.out.println("Executing this command: "+queryString+"\n");
      if(ps.executeUpdate() == 0){
        return false;
      }
      return true;
    } catch (SQLException ex){
      System.out.println("Could not update height");
      return false;
    }
  }

  public boolean updateDB(){
    try{
      queryString = "create table if not exists mostPopulousCountries (cid int, cname varchar(20))";
      //prep
      ps = connection.prepareStatement(queryString);
      System.out.println("Executing: "+ queryString);
      //execute
      ps.executeUpdate();

      //find most populous countries
      queryString = "select cid, cname from a2.country where population > 100000000";
      ps = connection.prepareStatement(queryString);
      System.out.println("Executing: "+ queryString);
      rs = ps.executeQuery();
      //insert every value into mostPopulousCountries
      if(rs.next()){
        int cid = rs.getInt(1);
        String cname = rs.getString(2);
        String sqlText = "insert into a2.mostPopulousCountries (cid, cname) values (?, ?)";
        //prep statement
        ps = connection.prepareStatement(sqlText);
        //set args
        ps.setInt(1, cid);
        ps.setString(2, cname);
        System.out.println("Executing: "+ sqlText);
        //execute
        if(ps.executeUpdate() == 0){
          return false;
        }

        return true;

      }
    } catch (SQLException ex){
      System.out.println("Could not find most populous countries");
      return false;
    }
    return false;
  }

  public static void correctUsage()
  {
      System.out.println("\nIncorrect number of arguments.\nUsage:\n "+
                         "java   \n");
      System.exit(1);
  }

/*
  public static void main (String args[])
  {
    Assignment2 demo = new Assignment2();
    demo.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3brunel", "g3brunel", "");

    if(demo.chgHDI(1233, 2009, 123) == false){
      System.out.println("didnt work!");
    }
    else{
      System.out.println("worked!@");
    }

  }
*/


}
