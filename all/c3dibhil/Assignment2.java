import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Assignment2 {
    
  Connection connection;
  Statement sql;
  ResultSet rs;
  
  Assignment2(){
	  try {
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
		
	}
  }

  public boolean connectDB(String URL, String username, String password){
      try {
		connection = DriverManager.getConnection(URL, username, password);
	} catch (Exception e) {
		return false;
	}
      return true;
  }
  
  public boolean disconnectDB(){
      try{
    	  connection.close();
      }
      catch(Exception e){
    	  return false;
      }
      return true;
  }

  public boolean insertCountry (int cid, String name, int height, 
		  int population) {
	  try{
		  sql = connection.createStatement();
		  String query = "SELECT cid FROM a2.country WHERE cid = '" + cid + "';";
		  rs = sql.executeQuery(query);
		  if(!rs.next()){
			  String sqlText = "INSERT INTO a2.country VALUES('" + name + "', '" 
					  			+ cid + "', '" + height + "', '"  
					  			+ population + "')";
			  sql.executeUpdate(sqlText);
		  }
		  else{

			  return false;
		  }
	   } 
	  catch(Exception e){
		   return false;
	   }
	  finally{
		  closeStatementAndResult();
	  }
	   return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int count = -1;
	  try{
		  String query = "SELECT count(cid) FROM a2.oceanAccess WHERE oid = '%1'".replaceAll("%1", ""+oid);
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);
		  if(rs.next()){
			  count = rs.getInt(1);
		  }
	   } 
	   catch(Exception e){
		   return -1;
	  }
	  finally{
		  closeStatementAndResult();
		  }

	  return count; 
  }
   
  public String getOceanInfo(int oid){
	  String result = "";
	  try{
		  String query = "SELECT oid, oname, depth FROM a2.ocean WHERE oid = '%1'".replaceAll("%1", oid+"");
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);
		  if(rs.next()){
			  result += rs.getString("oid") + ":" + rs.getString("oname") + ":"
					  	+ rs.getInt("depth");
		  }
	   } 
	  catch(Exception e){
		   return "";
	   }
	  finally{
		  closeStatementAndResult();
	  }
	   return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  String query = "UPDATE a2.hdi SET hdi_score = '%1' WHERE year = '%2' AND cid = '%3'"
			  .replaceAll("%1", ""+ newHDI)
			  .replaceAll("%2", ""+ year)
			  .replaceAll("%3", ""+ cid);
	  try{
		  sql = connection.createStatement();
		  sql.executeUpdate(query);
	  }
	  catch(Exception e){
		  return false;
	  }
	  finally{
		  closeStatementAndResult();
	  }
	  return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try{
		  sql = connection.createStatement();
		 
		  String text = "DELETE FROM a2.neighbour WHERE ('xx' = country AND 'zz' = "
		  		+ "neighbor) OR ('zz' = country AND 'xx' "
		  		+ "= neighbor)";
		  text = text.replace("xx", ""+c1id);
		  text = text.replace("zz", ""+c2id);
		  sql.executeUpdate(text);
	  }
	  catch(Exception e){
		  return false;
	  }
	  finally{
		  closeStatementAndResult();
	  }
	  return true;        
  }
  
  public String listCountryLanguages(int cid){
	  String result = "";
	  try{
		  String query = "SELECT lid, lname, lpercentage, population FROM a2.language NATURAL JOIN a2.country WHERE "
		  		+ "cid = '" + cid + "'";
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);

		  while(rs.next()){
			  result += rs.getInt("lid") + ":" + rs.getString("lname") + ":"
					  	+ (int)(rs.getFloat("lpercentage")*rs.getInt("population")) + "#";
		  }
	   } 
	  catch(Exception e){
		   return "";
	   }
	  finally{
		  closeStatementAndResult();
	  }
	   return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
	  String query = "SELECT height FROM a2.country WHERE cid = %1".replaceAll("%1", ""+cid);
	  int height = 0;
	  try{
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);
		  if(rs.next()){
			  height = rs.getInt("height");
		  }
		  else{
			  return false;
		  }
		  String query2 = "UPDATE a2.country SET height = %1 WHERE cid = %2".replaceAll("%2", cid+"").replaceAll("%1", ""+(height-decrH));
		  sql.executeUpdate(query2);
	  }
	  catch(Exception e){
	  }
	  finally{
		  closeStatementAndResult();
	  }
    return true;
  }
    
  public boolean updateDB(){
	  String query ="CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries " +
              "(id INTEGER not NULL, " +
              " cname VARCHAR(255)) ";
	  
	  String query2 = "SELECT cid, cname FROM a2.country WHERE population > 99999999";
	  
	  String query3 = "DELETE FROM a2.mostpopulouscountries WHERE 1=1";

	  List<Integer> cids = new ArrayList<Integer>();
	  List<String> cnames = new ArrayList<String>();
	  

	  try{
		  sql = connection.createStatement();
		  Statement sql2 = connection.createStatement();
		  sql.executeUpdate(query);
		  rs = sql.executeQuery(query2);
		  sql2.executeUpdate(query3);
		  sql2.close();
		  if(!rs.next()){
			  return true;
		  }
		  else{
			  cids.add(rs.getInt("cid"));
			  cnames.add(rs.getString("cname"));
			  while(rs.next()){
				  cids.add(rs.getInt("cid"));
				  cnames.add(rs.getString("cname"));
			  }
		  }
		  
		  for(int x = 0; x<cids.size(); x++){
			  String query4 = "INSERT INTO a2.mostPopulousCountries VALUES(%1, '%2')";
			  query4 = query4.replaceAll("%1", ""+cids.get(x));
			  query4 = query4.replaceAll("%2", cnames.get(x));
			  sql.executeUpdate(query4);
		  }
	  }
	  catch(Exception e){
		  return false;
	  }
	  finally{
		  closeStatementAndResult();
	  }
	  
	  return true;    
	
  }
  
  public void closeStatementAndResult(){
	  try{
		  if(sql != null){
			  sql.close();
		  }
		  if(rs != null){
			  rs.close();
		  }
	  }
	  catch(Exception e){
		  
	  }
  }
}