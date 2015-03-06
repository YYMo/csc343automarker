import java.sql.*;
import java.io.*;


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
                
                e.printStackTrace();
                return;
            }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	try {
          connection = DriverManager.getConnection("jdbc:postgresql://" + URL, username, password);     
	}
	catch(SQLException se){
            System.err.println(se);
		return false;
	}
return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          connection.close();
      }
      catch(SQLException se){
          System.err.println(se);
      return false;
      }
      return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      int ccid;
try{
        PreparedStatement findCID = connection.prepareStatement("SELECT cid FROM a2.country WHERE cid = ?");
        findCID.setInt(1, cid);
        ResultSet getCID = findCID.executeQuery();
        getCID.next();
        ccid = getCID.getInt(1);
        getCID.close();
        findCID.close();
        return false;
}
catch(SQLException se){
}
try{
    if (name.length() > 20){
        return false;
    }
	PreparedStatement countryStat = connection.prepareStatement("INSERT INTO a2.country(cid, cname, height, population) VALUES(?, ?, ?, ?)");

    countryStat.setInt(1, cid);
    countryStat.setString(2, name);
    countryStat.setInt(3, height);
    countryStat.setInt(4, population);
    countryStat.executeUpdate();
    
    countryStat.close(); 
  }
catch(SQLException se){
 System.err.println(se);
 return false;
 }
return true;
}
  
  public int getCountriesNextToOceanCount(int oid){ 
      int num;
     try {
         PreparedStatement besideOcean = connection.prepareStatement("SELECT count(cid) FROM a2.oceanAccess where oid = ?");
         besideOcean.setInt(1, oid);
         ResultSet countNextTo = besideOcean.executeQuery();
         countNextTo.next();
         num = countNextTo.getInt(1);
         
         besideOcean.close();
         countNextTo.close();
     }
     catch(SQLException se){
        System.err.println(se);
	return -1;
     }
     return num;
  }
   
  public String getOceanInfo(int oid){
      int oidnum;
      String name;
      int depth;
      try{
         PreparedStatement findOID = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
         findOID.setInt(1, oid);
         ResultSet OceanInfo = findOID.executeQuery();
         OceanInfo.next();
         oidnum = OceanInfo.getInt(1);
         name = OceanInfo.getString(2);
         depth = OceanInfo.getInt(3);
         
         findOID.close();
         OceanInfo.close();
      }
      catch(SQLException se){
          System.err.println(se);
          return "";
      }
   String info = oidnum + ":" + name + ":" + depth;
   return info;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try{
          //check if cid and year exist
          int ccid;
          int cyear;
          PreparedStatement check_exists = connection.prepareStatement("SELECT cid, year FROM a2.hdi WHERE cid = ? AND year = ?");
          check_exists.setInt(1, cid);
          check_exists.setInt(2, year);
          ResultSet hdi_info = check_exists.executeQuery();
          hdi_info.next();
          ccid = hdi_info.getInt(1);
          cyear = hdi_info.getInt(2);
          check_exists.close();
          hdi_info.close();  
      }
      catch(SQLException se){
          System.out.println("cid and/or year does not exist");
          return false;
      }
      try{
          PreparedStatement updateHDI = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
          updateHDI.setFloat(1, newHDI);
          updateHDI.setInt(2, cid);
          updateHDI.setInt(3, year);
          updateHDI.executeUpdate();
          updateHDI.close();
      }
      catch(SQLException se){
          System.err.println(se);
          return false;
      }
   return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try{
          PreparedStatement delete = connection.prepareStatement("DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?)"
                  + " OR (country = ? AND neighbor = ?)");
      delete.setInt(1, c1id);
      delete.setInt(2, c2id);
      delete.setInt(3, c2id);
      delete.setInt(4, c1id);
      delete.executeUpdate();
      delete.close();
      }
      catch(SQLException se){
          System.err.println(se);
          return false;
      }
   return true;        
  }
  
  public String listCountryLanguages(int cid){
      String name;
      int pop;
      int langid;
      String lname;
      float percent;
      float langpop;
      String answer;
      
      //get country name and population
      try{
          PreparedStatement getconandpop = connection.prepareStatement("SELECT cname, population FROM a2.country WHERE cid = ?");
          getconandpop.setInt(1, cid);
          ResultSet countryInfo = getconandpop.executeQuery();
          countryInfo.next();
          name = countryInfo.getString(1);
          pop = countryInfo.getInt(2);
          
          getconandpop.close();
          countryInfo.close();
      }
      catch(SQLException se){
          System.err.println(se);
          return "";
      }
      //get's the lid and percentage for each language. Calculates population that speaks that language
      //and appends to info to the string answer
      try{
         PreparedStatement getLanguages = connection.prepareStatement("SELECT lid, lname, lpercentage FROM a2.language WHERE cid = ? ORDER BY lpercentage"); 
         getLanguages.setInt(1,cid);
         ResultSet languageInfo = getLanguages.executeQuery();
         
         answer = "";
         while(languageInfo.next()){
             langid = languageInfo.getInt(1);
             lname = languageInfo.getString(2);
             percent = languageInfo.getFloat(3);
             langpop = pop * percent;
             answer = answer + "|" + langid + ":|" + lname + ":|" + langpop + "#"; 
         }
         getLanguages.close();
         languageInfo.close();
         answer = answer.substring(0, answer.length()-1);
      } 
      catch(SQLException se){
         System.err.println(se);
         return "";
      }
	return answer;
  }
  
  public boolean updateHeight(int cid, int decrH){
      int cheight;
      int newheight;
      try{
      //get the current height of te country with cid
      PreparedStatement getheight = connection.prepareStatement("SELECT height FROM a2.country WHERE cid = ?");
      getheight.setInt(1, cid);
      ResultSet result = getheight.executeQuery();
      result.next();
      cheight = result.getInt(1);
      getheight.close();
      result.close();
      
      newheight = cheight - decrH; //decrease height
      
      //updates height
      PreparedStatement chngH = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?");
      chngH.setInt(1, newheight);
      chngH.setInt(2, cid);
      chngH.executeUpdate();
      chngH.close();
      }
      catch(SQLException se){
        System.err.println(se);
        return false;
      }
      return true;
  }
    
  public boolean updateDB(){
        int cur_cid;
        String cur_cname;
        
        try{
            //create table
            String newTable = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER PRIMARY KEY, cname varchar(20) NOT NULL)";
            PreparedStatement createTable = connection.prepareStatement(newTable);
            createTable.executeUpdate();
            createTable.close();
        }
        catch(SQLException se){
           System.err.println(se);
           return false;
        }
        
        try{
           //get most populous countries
            PreparedStatement getMostPop = connection.prepareStatement("SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid asc");
            ResultSet mostPop = getMostPop.executeQuery();
            
            //insert each tuple from mostPop into newly created table
            PreparedStatement insertInfo = null;
            while(mostPop.next()){
                cur_cid = mostPop.getInt(1);
                cur_cname = mostPop.getString(2);
                insertInfo = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries VALUES( ?, ?)");
                insertInfo.setInt(1, cur_cid);
                insertInfo.setString(2, cur_cname);
                insertInfo.executeUpdate();
            }
            getMostPop.close();
            mostPop.close();
            if (insertInfo != null){
            insertInfo.close();
            }
        }
        catch(SQLException se){
            System.err.println(se);
            return false;  
        }
    return true;

  }
  
  
}
