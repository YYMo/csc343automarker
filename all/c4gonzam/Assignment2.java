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
     try{
        Class.forName("org.postgresql.Driver");
     } catch(ClassNotFoundException e){
        e.printStackTrace();
        return;
      }
    }
  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
    try{
        connection = DriverManager.getConnection(URL, username, password);
        return true;        
    } catch (SQLException ee){
        return false;
    }
    
    }
  
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
    try{
        rs.close();
        sql.close();
        connection.close();
        return true;    
    } catch (SQLException e){
        //e.printStackTrace();
        return false;
    }
    }
    
    public boolean insertCountry (int cid, String name, int height, int population) {
    try{
        sql=connection.createStatement();
        String query1= "insert into a2.country" + " values (" + cid + ", '" + name + "'," + height + "," +population+ ")";
        sql.executeUpdate(query1);
        return true; 
    } catch (SQLException e){
        return false;
    }
    }

    public int getCountriesNextToOceanCount(int oid) {
    try{
        sql=connection.createStatement();
        String query2 = "select count(cid) from a2.oceanAccess where oid=" + oid ;
        rs = sql.executeQuery(query2);
        while (rs.next()){
        int s= rs.getInt("count");
        return s;
        }
    } catch (SQLException e){ 
      return -1;
    }
    return -1;
    }

    public String getOceanInfo(int oid){
    try{
        sql=connection.createStatement();
        String query3 = "select * from a2.ocean where oid=" + oid;
        rs= sql.executeQuery(query3);
        while (rs.next()){
        int a= rs.getInt("oid");
        String b= rs.getString("oname");
        int c= rs.getInt("depth");
        return a+":"+b+":"+c;
        }
    } catch (SQLException e){
        return "";
    }
    return "";
    }

    public boolean chgHDI(int cid, int year, float newHDI){
    try{
        sql=connection.createStatement();
        String query4= "update a2.hdi " + "set hdi_score= "+newHDI+ " where cid= "+cid +"and year= " +year;
        int s = sql.executeUpdate(query4);
        if (s != 0){
        return true;
        } else {
        return false;
            }
    } catch (SQLException e){
        return false;
    }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
    try{
        sql=connection.createStatement();
        String query5 = "delete from a2.neighbour "+ " where country= " +c1id+ "and neighbor= " +c2id;
        int a = sql.executeUpdate(query5);
        String query5b = "delete from a2.neighbour " + " where country= " +c2id+ "and neighbor= " +c1id;
        int b = sql.executeUpdate(query5b);
        if (a != 0 && b != 0){
                return true;
            } else {
                return false;
        }
        } catch (SQLException e){
            return false;
       }      
    }

    public String listCountryLanguages(int cid){
    try{
        String query6 = "select lid, lname, population*lpercentage as population from a2.language natural join a2.country where cid= " +cid+ "order by population*lpercentage";
        rs = sql.executeQuery(query6);
        if (rs != null){
        String l="";
        while (rs.next()){
            String lid = rs.getString("lid");
            String lname = rs.getString("lname");
            double population = rs.getDouble("population");
            l= l+ lid + ":" +lname+ ":" +population+ "#"; 
            }
        return l.substring(0, l.length()-1);
      } else {
            return "";
        }
        } catch (SQLException e){
        return "";
        }
    }

    public boolean updateHeight(int cid, int decrH){
    try{
        String query7 = "update a2.country "+ "set height = (select height from a2.country where cid= "+cid+") - " +decrH+ "where cid= " +cid;
        sql = connection.createStatement();
        int a = sql.executeUpdate(query7);
        if (a !=0){
        return true;
        } else {
        return false;
        }
    } catch (SQLException e){
        return false;
    }
    }

    public boolean updateDB(){
    try{
        sql=connection.createStatement();
        String query8 = "create table a2.mostPopulousCountries " + "(cid integer, " + " cname varchar(20))";
        sql.executeUpdate(query8);
        String query8a = "insert into a2.mostPopulousCountries select cid, cname from a2.country where population>600 order by cid ASC";
        sql.executeUpdate(query8a);
        return true;
    } catch (SQLException e){
        //    e.printStackTrace();      
        return false;
    }
}

    /*public static void main(String args[]){
    Assignment2 a2 = new Assignment2();
    boolean b = a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c4gonzam", "c4gonzam", "");
    boolean c = a2.insertCountry(12, "Michoacan" , 12, 120);
    int d = a2.getCountriesNextToOceanCount(1);
    String e = a2.getOceanInfo(4);
    boolean f = a2.chgHDI(1,2009,23f);  
    boolean g = a2.deleteNeighbour(1, 2);
    String h = a2.listCountryLanguages(1);
    boolean i = a2.updateHeight(30,-200);
    boolean j = a2.updateDB();
    System.out.println("connect " +b);
    System.out.println("insertCountry " +c);
    System.out.println("getCountriesNextToOceanCount " +d);
    System.out.println("getOceanInfo " +e);
    System.out.println("chgHDI "+f);   
    System.out.println("deleteNeighbour " +g);
    System.out.println("listCountryLanguages " +h);
    System.out.println("updateHeight "+i);
    System.out.println("update "+j);
    boolean k = a2.disconnectDB();       
    System.out.println("disconnect "+k);    
    } */ 
}