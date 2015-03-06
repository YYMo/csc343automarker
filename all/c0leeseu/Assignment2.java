import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  public static Connection connection = null;
  
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
  	  }catch (ClassNotFoundException e){
  	  	  System.out.println("no jdbc driver found");
  	  	  e.printStakTrace();
  	  	  return;
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
  	  try{
  	  	  connection = DriverManager.getConnection(URL, username, password);
  	  	  return true;
  	  }catch(SQLException e){
  	  	  System.out.println("connection failed");
  	  	  e.printStakTrace();
  	  	  return false;
  	  }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
  	  try{
  	  	  connection.close();
  	  	  return true;
  	  }catch (SQLException e){
  	  	  System.out.println("disconnect failed");
  	  	  e.printStackTrace();
  	  	  return false;
  	  }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
  	  string text;
  	  boolean ans = false;
  	  try{
  	  	  sql = connection.createStatement();
  	  	  //check if cid exists
  	  	  text = "select cid from country where cid = "+ cid" ";
  	  	  rs = sql.excuteQuery(text);
  	  	  
  	  	  //rs.next() gives false if theres no searched data in rs
  	  	  if (rs.next()== false){
  	  	  	  text = "insert into country values("+cid+"," +name+"," +height+"," +population+")";
  	  	  	  sql.excuteUpdate(text);
  	  	  	  ans= true;
  	  	  }
  	  }catch(SQLException e){
  	  	  Systyem.err.println("error");
  	  	   e.printStackTrace();
  	  }sql.close();
  	  rs.close();
  	  
   return ans;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
  	  string text;
  	  int ans;
  	  sql = connection.createStatement();
  	  text = "select count(cid)as counter from oceanaccess where oid = "+ oid +" ";
  	  rs = sql.excuteQuery(text);
  	  if (rs.next()){
  	  	  ans = rs.getInt(1);
  	  }else{
  	  	  ans = -1;
  	  }sql.close();
  	  rs.close();
	return ans;  
  }
   
  public String getOceanInfo(int oid){
  	  string text;
  	  string ans;
  	  sql = connection.createStatement();
  	  text = "select oid, oname, depth from ocean where oid = "+oid+" "
  	  rs = sql.excuteQuery(text);
  	  if (rs.next()){
  	  	  ans = rs.getString("oid"+":"+"oname"+":"+"depth");
  	  }else{
  	  	  ans = "";
  	  }sql.close();
  	  rs.close();
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   return false;        
  }
  
  public String listCountryLanguages(int cid){
	return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    return false;
  }
    
  public boolean updateDB(){
	return false;    
  }
  
}
