import java.sql.*;
import java.util.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps, ps2;
  
  // Resultset for the query
  ResultSet rs;
  
  String queryString;
  
  //CONSTRUCTOR
  
  Assignment2(){
	  try{
		  Class.forName("org.postgresql.Driver");
	  }catch (ClassNotFoundException e){
		  return;
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try{
		  connection = DriverManager.getConnection(URL, username, password);
	
	  } catch (SQLException e){
		  return false;
	  }
	  if(connection != null){
		  return true;
	  }else{
		  return false;
	  }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB()throws SQLException{
	  try{	
	      connection.close();
          return true;   
	  }catch (SQLException e){
		  return false;
	  }	 
  }
    
  public boolean insertCountry (int cid, String name, int height, int population)throws SQLException{
	  try{
		  queryString = "INSERT INTO A2.country(cid, cname, height, population) VALUES (?, ?, ?, ?)";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  int result = ps.executeUpdate();
		  if(result == 1){
			  return true;
		  }else{
			  return false;
		  }
	  }catch(SQLException se){
		  return false;
	  }finally{
		  ps.close();
	  }

  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException{
	  try{
		  queryString = "select count(cid) as num from A2.oceanaccess where oid = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  
		  if(rs.next()){
			  int count = rs.getInt("num");
			  return count;
		  }
		  else{
			  return -1;
		  }
	  }catch(SQLException se){
		  return -1;
	  }
	  finally{
	      rs.close(); 
          ps.close();
	  }		
  }
   
  public String getOceanInfo(int oid)throws SQLException{
	  try{
		  String OceanInfo = "";
		  queryString = "Select * from A2.ocean where ocean.oid = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  if(rs.next()){
			  OceanInfo = OceanInfo + rs.getString("oid") + ":" + rs.getString("oname") + ":" + rs.getString("depth"); 
		  }else{
			  OceanInfo = "";
		  }
		  return OceanInfo;
	  }catch(SQLException se){
		   return "";
      }finally{
		  ps.close();
          rs.close();
	  }
  }

  public boolean chgHDI(int cid, int year, float newHDI)throws SQLException{
	  try{
		  queryString = "Update A2.hdi SET hdi_score = ? where cid = ? and year = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  int result = ps.executeUpdate();
	      if(result == 1){
		      return true;
	      }else{
			  return false;
		  }
	  }catch(SQLException se){
		  return false;
	  }
	  finally{
		  ps.close();
      }		 
  }

  public boolean deleteNeighbour(int c1id, int c2id)throws SQLException{
	  try{
	  	queryString = "Delete from A2.neighbour where (country = ? and neighbor = ?) or (country = ? and neighbor = ?)";
	  	ps = connection.prepareStatement(queryString);
	  	ps.setInt(1, c1id);
	  	ps.setInt(2, c2id);
	  	ps.setInt(3, c2id);
	  	ps.setInt(4, c1id);
	  	int result = ps.executeUpdate();
	  	if(result == 2){
		      return true;
	      }else{
			  return false;
		  }
	  }catch(SQLException se){
		return false;	
	  }	 
	  finally{
		ps.close();
	  }       
  }
  
  public String listCountryLanguages(int cid)throws SQLException{
	  try{
		  queryString = "select language.lid, language.lname, (language.lpercentage  * country.population) as lpop" +
		  " from A2.language join A2.country on language.cid = country.cid where country.cid = ? order by lpop";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  String result = "";
			  while(rs.next()){
				  result += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getString("lpop");
				  result += "#";	
		      }
			  result = result.substring(0, result.length() - 1);
			  return result;
	  }catch (SQLException se){
	      return "";
	  }finally{
		  ps.close();
		  rs.close();
	  }	

  }
  
  public boolean updateHeight(int cid, int decrH)throws SQLException{
	  int height;
	  try{
	 	 String qString = "select height from A2.country where cid = ?";
	 	 ps = connection.prepareStatement(qString);
	 	 ps.setInt(1, cid);
	 	 rs = ps.executeQuery();
		 if(rs.next()){
		     height = rs.getInt("height");
		 }else{
			 return false;
		 }
	  }catch (SQLException se){
		 return false;
      }finally{	 
	 	 ps.close();
		 rs.close();
	  }
	  try{	
		  queryString = "Update A2.country SET height = ? where cid = ?";
		  ps = connection.prepareStatement(queryString);
		  ps.setInt(1, height - decrH);
		  ps.setInt(2, cid);
		  int result = ps.executeUpdate();
		  if(result == 1){
		      return true;
	      }else{
			  return false;
		  }
	  }catch (SQLException se){
			return false;
	  }finally{	
		  ps.close();
	  }

  }
    
  public boolean updateDB()throws SQLException{
	   String createText;
	   String selectText;
	   String insertText;
	   HashMap<Integer, String> result = new HashMap<Integer, String>();
	   createText = "create table A2.mostPopularCountries (" +
	   		      "cid integer references A2.country(cid) ON DELETE RESTRICT," +
			      "cname varchar(20)," +
	   		      "primary key(cid))";
	   selectText = "select cid, cname from A2.country where population>100000000 order by cid ASC";
	   insertText = "INSERT INTO A2.mostPopularCountries VALUES (?,?)";
	   		      
	  try{
		  sql = connection.createStatement();
		  sql.executeUpdate(createText);
		  
	  }catch(SQLException se){
		  return false;
	  }
	  finally{
		sql.close();
	  }		
	  try{
		  ps = connection.prepareStatement(selectText);	  
		  rs = ps.executeQuery();
		  while(rs.next()){
			  result.put(rs.getInt("cid"), rs.getString("cname"));
		  }
	  }catch(SQLException se){
		  return false;
	  }	  
	  finally{
		 rs.close();
		 ps.close();
	  }
	  try{
		  ps = connection.prepareStatement(insertText);
		  List<Integer> sortedKeys = new ArrayList<Integer>(result.keySet());
		  Collections.sort(sortedKeys);
		  for(int cid : sortedKeys){
			  ps.setInt(1, cid);
			  ps.setString(2,result.get(cid));
			  ps.executeUpdate();
		  }
		  return true;
	  }catch(SQLException se){
		  return false;
	  }	  
	  finally{
		 ps.close();
	  }
  }
  
}
