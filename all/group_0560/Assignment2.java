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
		//e.printStackTrace();
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		connection = DriverManager.getConnection(URL, username, password);
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
		connection.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try{
		  sql=connection.createStatement();
		  String sqlText;
		  String sqlCheck;
		  Boolean retVal;
		  sqlCheck = "SELECT * FROM a2.country WHERE cid =" + cid;
		  rs = sql.executeQuery(sqlCheck);
		  if(rs.next()){
			  retVal = false;
		  }
		  else{
			  sqlText = "INSERT INTO a2.country " + "VALUES(" + cid + ",'" + name + "'," + height + ","+ population + ")";
			  sql.executeUpdate(sqlText);
			  retVal = true;
		  }
		  rs.close();
                  return retVal;
	  }catch (SQLException e){
		  return  false;
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try{
		  ps = connection.prepareStatement("select count(cid) from a2.oceanAccess where oid = ?");
		  ps.setInt(1, oid);
		  
		  ResultSet countries = ps.executeQuery();
		  int countriesnext = 0;
		  
		  while(countries.next()){
			  countriesnext = countries.getInt(1);
		  }
		  
		  countries.close();
		  return countriesnext;
	  } catch (SQLException e){
		  return -1;
	  }
  }
   
  public String getOceanInfo(int oid){
	  try{
		  String retVal = "";
		  sql = connection.createStatement();
		  String sqlText;
		  
		  sqlText = "SELECT *       " +
                  " FROM a2.ocean " +
				  " WHERE oid = " + oid;
       	  rs = sql.executeQuery(sqlText);
       	  if (rs != null){
       		  while (rs.next()){
       			  retVal = rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth");
       		  }
       	  }
       	  //Close the resultset
       	  rs.close();
	  return retVal;		  
	  }catch(SQLException e){
		  return "";
	  }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try{
		  ps = connection.prepareStatement("update a2.hdi set hdi_score = ? where cid = ? and year = ?");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  ps.executeUpdate();
		  ps.close();
		  return true;
	  } catch(SQLException e){
		  return false;
	  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
		  Boolean retVal=false;
		  sql = connection.createStatement();
		  String sqlText,sqlText2;
		  sqlText = "DELETE FROM a2.neighbour WHERE country =  " + c1id 
				  + " AND neighbor = " + c2id;
		  sqlText2 = "DELETE FROM a2.neighbour WHERE country = " 
				  + c2id + " AND neighbor = " + c1id;
		  if (sql.executeUpdate(sqlText)!=0 && sql.executeUpdate(sqlText2)!=0){
			  retVal = true;
   		  }
		   return retVal;
	  }catch(SQLException e){
		  return false;
	  }
  }
  
  public String listCountryLanguages(int cid){
	  int population;
	  int totalpopulation = 0;
	  String ans = "";
	  
	  try {
		ps = connection.prepareStatement("select population from a2.country where cid = ?");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		while(rs.next()){
			totalpopulation = rs.getInt(1);
		}
		ps.close();
		rs.close();
		
		ps = connection.prepareStatement("select lid, lname, lpercentage from a2.language where cid = ? order by lpercentage DESC");
		ps.setInt(1, cid);
		
		rs = ps.executeQuery();
		
		boolean notLast = rs.next();
		while(notLast){
			ans = ans.concat(rs.getString(1));
			ans = ans.concat(":");
			ans = ans.concat(rs.getString(2));
			ans = ans.concat(":");
			population = (int) (rs.getFloat(3) * totalpopulation);
			ans = ans.concat(Integer.toString(population));
			notLast = rs.next();
			if(notLast){
				ans = ans.concat("#");
			}
		}
		
		ps.close();
		rs.close();
		
		return ans;
		
		
	} catch (SQLException e) {
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try{
		  Boolean retVal = false;
		  sql = connection.createStatement();
		  String sqlText,sqlQuery;
		  int height=0;
		  sqlQuery = "SELECT * FROM a2.country WHERE cid = "+ cid;
		  rs = sql.executeQuery(sqlQuery);
		  if (rs != null){
       		  while (rs.next()){
       			  height = rs.getInt("height");
       		  }
       	  }
		  sqlText = "UPDATE a2.country      " +
                  "   SET height = " + (height-decrH) + 
                  " WHERE  cid = " + cid; 
		  sql.executeUpdate(sqlText);
		  rs.close();
		  return true;
	  }catch(SQLException e){
		  return false;
	  }
  }
    
  public boolean updateDB(){
	  try{
		  ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries (cid INTEGER not NULL, cname VARCHAR(20), PRIMARY KEY(cid))");
		  ps.executeUpdate();
		  ps.close();
		  
		  ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries SELECT cid, cname FROM a2.country where population > 100000000 order by cid ASC");
		  ps.executeUpdate();
		  ps.close();
		  
		  return true;
	  }catch(SQLException e){
		  return false;
	  }  
  }
  
}
