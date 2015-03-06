/* 
 * FILES:   TestAssignment2.java 
 * CLASSNAME:   TestAssignment2
 * DESCRIPTION: Test driver for testing JDBC part of CSC343 Assignment2 (JDBC part)
 * MISC:    1) Assumes that classpath is configured to 
 *         include postgresql driver
 */

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.*;
import java.util.Vector;
import java.util.Arrays;
import java.util.Comparator;

class TestAssignment2
{
    Connection conn;
    Assignment2 a2;
    
    String method[] = new String[10];   /* stores the method name */
    int result[] = new int[10];   /* stores success status of each method */
    double marks = 0;       /* stores marks for cases */

    // constructor
    TestAssignment2()
    {
        conn = null;
        try{
        a2 = new Assignment2();
        }catch(Exception s)
        {   
            System.out.println("Error");
        }

        method[0] = "connectDB()";
        method[1] = "insertCountry()";
        method[2] = "getCountriesNextToOceanCount()";
        method[3] = "getOceanInfo()";
        method[4] = "chgHDI()";
        method[5] = "deleteNeighbour()";
        method[6] = "listCountryLanguages()";
        method[7] = "updateHeight()";
        method[8] = "updateDB()";
        method[9] = "disconnectDB()";

        for ( int i = 0; i < 10; i++ )
            result[i] = 0;
    }

    public static void main(String args[])
    {
        // check for number of arguments
        if ( args.length < 2 || args.length > 3 )
        {
            System.out.println("Usage: java TestAssignment2 dbname " + 
                    "username [password]");
            System.exit(1);
        }

        TestAssignment2 ta2 = new TestAssignment2();
        int sum = 0;
        // test all methods one at a time
        if(args.length==3)
            ta2.result[0] = ta2.TestconnectDB(args[0], args[1], args[2]);
        else
            ta2.result[0] = ta2.TestconnectDB(args[0],args[1],"");

        System.out.println("\nJDBC connectDB Mark: " + ta2.getTestStatus(ta2.result[0], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[0];
        ta2.result[1] = ta2.TestinsertCountry();
        System.out.println("\nJDBC insertCountry Mark: " + ta2.getTestStatus(ta2.result[1], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[1];
        ta2.result[2] = ta2.TestgetCountriesNextToOceanCount();    
        System.out.println("\nJDBC getCountriesNextToOceanCount Mark: " + ta2.getTestStatus(ta2.result[2], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[2];
        ta2.result[3] = ta2.TestgetOceanInfo();      
        System.out.println("\nJDBC getOceanInfo Mark: " + ta2.getTestStatus(ta2.result[3], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[3];
        ta2.result[5] = ta2.TestdeleteNeighbour();      
        System.out.println("\nJDBC deleteNeighbour mark: " + ta2.getTestStatus(ta2.result[5], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[5];
        ta2.result[6] = ta2.TestlistCountryLanguages();        
        System.out.println("\nJDBC listCountryLanguages Mark: " + ta2.getTestStatus(ta2.result[6], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[6];
        ta2.result[4] = ta2.TestchgHDI(); // affects listcourses
        System.out.println("\nJDBC chgHDI Mark: " + ta2.getTestStatus(ta2.result[4], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[4];
        ta2.result[7] = ta2.TestupdateHeight();       
        System.out.println("\nJDBC updateHeight Mark: " + ta2.getTestStatus(ta2.result[7], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[7];
        ta2.result[8] = ta2.TestupdateDB(); //run ammendgrades as the last test     
        System.out.println("\nJDBC updateDB Mark: " + ta2.getTestStatus(ta2.result[8], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[8];
        ta2.result[9] = ta2.TestdisconnectDB();
        System.out.println("\nJDBC disconnectDB Mark: " + ta2.getTestStatus(ta2.result[9], 5));
        System.out.println("=============================================\n");
        sum += ta2.result[9];        
        
        String part2 = "JDBC Part Total Mark : " + sum +" /50";
        System.out.println(part2);
        System.out.println("=============================================\n");

    }

    int TestconnectDB(String dbname, String username, String password)
    {
        int case_marks = 0;
        try
        {
            String url = "jdbc:postgresql:" + dbname;

            // Establish our own connection to database
            // (for verification/testing purposes)
            conn = DriverManager.getConnection(url, username, password);

            System.out.println("-----------------------------------");
            System.out.println("Testing connectDB() method...");
            System.out.println("-----------------------------------");
            boolean success = a2.connectDB(url, username, password);
            System.out.println("Return Value: " + success);
            System.out.println("Correct Value: " + true);
            if ( success == true ){
                case_marks++;
            }
        }
        catch (SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks*5;
    }

    int TestinsertCountry()
    {
        int case_marks = 0;
        try
        {
            System.out.println("-----------------------------------");
            System.out.println("Testing insertCountry() method...");
            System.out.println("-----------------------------------");
//          System.out.println("Inserting record ('5200', 'Sword', 'Skyward', '27', 'M', 'Computer Science', '3')");
            
            // verify from database directly
            Statement stmt = null;
            ResultSet rs = null;
            int stud_count = 0;
            int new_stud_count = 0;
            String queryString;

            // verify total no of rows and newly inserted from department table
            stmt = conn.createStatement();
            queryString = "select count(*) from a2.country";
            rs = stmt.executeQuery(queryString);

            if ( rs.next() )
                stud_count = rs.getInt(1);

            System.out.println("Inserting record (1, \"Utopia\", 4, 2)");
            boolean success1 = a2.insertCountry(1, "Utopia", 4, 2);
            
            System.out.println("Return Value: " + success1);
            System.out.println("Correct Value: " + true);
            System.out.println();
            if (success1) case_marks+=3;
            
            // insert a country with invalid fields...
            System.out.println("Inserting record (276, \"FakeOne\", 1, 1 )");
            boolean success2 = a2.insertCountry(276, "FakeOne", 1, 1);
            System.out.println("Return Value: " + success2);
            System.out.println("Correct Value: " + false);
            System.out.println();
            if (!success2) case_marks++;


            // verify from database directly
            stmt = null;
            rs = null;

            // verify total no of rows and newly inserted from department table
            stmt = conn.createStatement();
            queryString = "select count(*) from a2.country";
            rs = stmt.executeQuery(queryString);

            if ( rs.next() ) {
                new_stud_count = rs.getInt(1);
                System.out.println("Number of Insertions: ");
                System.out.println("Return Value: " + (new_stud_count - stud_count));
                System.out.println("Correct Value: " + 1);

                if (new_stud_count - stud_count == 1) case_marks++;
            }
        }
        catch (SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }

    int TestgetCountriesNextToOceanCount()
    {
        int case_marks = 0;
        try{
            System.out.println("-----------------------------------");
            System.out.println("Testing getCountryNextToOceanCount() method...");
            System.out.println("-----------------------------------");
            
            int correct_count = 0;
            //Canada
            int count1 = a2.getCountriesNextToOceanCount(1);
            System.out.println("Return value: " + count1);
            // verify from database directly
            Statement stmt = null;
            ResultSet rs = null;
            String queryString;

            // verify total no of rows and newly inserted from department table
            stmt = conn.createStatement();
            queryString = "select count(*) from a2.oceanAccess where oid = 1";
            rs = stmt.executeQuery(queryString);

            if ( rs.next() )
                correct_count = rs.getInt(1);

            //ADDED by t1hussai
            // checks if proper number obtained
            System.out.println("Correct Value: " + correct_count);
            System.out.println();
            if (count1 == correct_count) case_marks += 3;

            System.out.println("Getting # for non-existing country...");

            int count2 = a2.getCountriesNextToOceanCount(555);

            System.out.println("Return value: " + count2);
            System.out.println("Correct Value: " + -1);

            if (count2 == -1) case_marks += 2;

        }catch (SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }


    int TestgetOceanInfo()
    {
        int case_marks = 0;
        try{
            System.out.println("-----------------------------------");
            System.out.println("Testing getOceanInfo() method...");
            System.out.println("-----------------------------------");
            System.out.println("Getting info for Ocean with pid = 3...");

            String info = a2.getOceanInfo(3); //Indian Ocean
            String temp = info.replaceAll(" +", " ").replaceAll(" :", ":");
            System.out.println("Return value: \n" + info);
                        
            System.out.println("\nCorrect Value: \n" + "3:Indian:8047");
            System.out.println();
            if (temp.equals("3:Indian:8047"))case_marks += 3;

            // testing non existent Ocean
            System.out.println("Getting info for Ocean with cid = 10 ... (no such Ocean)");
            String info2 = a2.getOceanInfo(10);
            System.out.println("Return value: " + info2.isEmpty());
            System.out.println("Correct Value: " + true);

            if (info2.isEmpty()) case_marks += 2;

        }catch (Exception se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }   


    int TestdeleteNeighbour()
    {
        int case_marks = 0;
        try 
        {
            //Deleting the country we inserted during testing insertCountry() function...
            System.out.println("-----------------------------------");
            System.out.println("Testing deleteNeighbour() method...");
            System.out.println("-----------------------------------");
            System.out.println("Delete the neighbouring relations between Russia and Kanakhstan which is existed");



            boolean success1 = a2.deleteNeighbour(643, 398);

            System.out.println("Return Value: " + success1);
            System.out.println("Correct Value: " + true);
            System.out.println();
            if (success1) case_marks+=2;

            System.out.println("Delete the neighbouring relations between Russia and USA which is not existed");

            boolean success2 = a2.deleteNeighbour(643, 840);

            System.out.println("Return Value: " + success2);
            System.out.println("Correct Value: " + false);

            if (!success2) case_marks+=2;

            // verify from database directly
            Statement stmt = null;
            ResultSet rs = null;
            int KZ_count = 0;
            int RU_count = 0;
            String queryString;

            // verify total no of rows from countries table
            stmt = conn.createStatement();
            queryString = "select count(*) from a2.neighbour where country=398";
            rs = stmt.executeQuery(queryString);

            if ( rs.next() )
                KZ_count = rs.getInt(1);

            queryString = "select count(*) from a2.neighbour where country=643";
            rs = stmt.executeQuery(queryString);

            if ( rs.next() )
                RU_count = rs.getInt(1);

            if ( success1 == true && success2 == false && 
                    KZ_count == 2 && RU_count == 1){
                case_marks++;
            }
        }
        catch (SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }

    
    // This method compares output against hard coded values
    int TestchgHDI()
    {
        int case_marks = 0;
        try{

        System.out.println("-----------------------------------");
        System.out.println("Testing chgHDI() method...");
        System.out.println("-----------------------------------");
        System.out.println("Change HDI with a legal value");
        boolean retval = a2.chgHDI(156, 2009, (float)0.5);
        System.out.println("Return Value: " + retval);
        System.out.println("Correct Value: " + true);

        Statement stmt = null;
        ResultSet rs = null;
        int y = 0;
        String queryString = "select year from a2.hdi where cid = 156 and hdi_score=0.5";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(queryString);

        if ( rs.next() )
            y= rs.getInt(1);
        if(retval){
            case_marks += 1;
        }

        if (y == 2009){
            System.out.println("Validate Value is correct in DB");
            case_marks += 3;
        }
        else{
            System.out.println("Validate Value is not correct in DB");
        }

        System.out.println("chgHDI(157, 2009, (float)0.5)");
        boolean retval3 = a2.chgHDI(157, 2009, (float)0.5);
        System.out.println("Return Value: " + retval3);
        System.out.println("Correct Value: " + false);
        System.out.println();


        if(!retval3){
            case_marks += 1;
        }

        }catch(SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }

    // This method lists the courses of a student with a given sid
    int TestlistCountryLanguages()
    {
        int case_marks = 0;
        try{
            System.out.println("-----------------------------------");
            System.out.println("Testing listCountryLanguages() method...");
            System.out.println("-----------------------------------");
            System.out.println("Listing languages with cid = 356...");
            String courses = a2.listCountryLanguages(356);

            //////CHANGED by t1hussai
            String [] temp = courses.replaceAll(" +", " ").replaceAll(" :", ":").split("#");
            Arrays.sort(temp, String.CASE_INSENSITIVE_ORDER);
            System.out.println("Return Value: ");
            for (int i = 0; i < temp.length; i++) {
                System.out.print(temp[i].toString());
            }
            System.out.println();
            
            String [] vec = new String[] {
                  "6:Hindi:504800000",
                  "12:Bengali:126200000",
                  "13:Telugu:88340000",
                  "3:English:12620000"
                };
            
            Arrays.sort(vec, String.CASE_INSENSITIVE_ORDER);
            System.out.println("\nCorrect Value: ");
            for (int i = 0; i < vec.length; i++) {
                System.out.print(vec[i].toString());
            }
            System.out.println("\n");

            boolean matchup = true;
            if (vec.length == temp.length){
                case_marks += 1;
                if(temp[0].indexOf("6:Hindi") != -1
                    && temp[1].indexOf("12:Bengali") != -1
                    && temp[2].indexOf("13:Telugu") != -1
                    && temp[3].indexOf("3:English") != -1){

                }{
                    case_marks += 3;
                }
            }

            for (int i = 0; i < vec.length; i++) {
                if (!vec[i].equals(temp[i]))
                  matchup = false;
            }
            if (matchup)
              case_marks += 0;
            
            System.out.println("Listing languages with cid = 10...(which does not exist)");
            String info2 = a2.listCountryLanguages(10);
            System.out.println("Return value: " + info2.isEmpty());
            System.out.println("Correct Value: " + true);
            System.out.println();
            if (info2.isEmpty())
                case_marks += 1;

        }catch (Exception se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
        }
        return case_marks;
    }
    
    // This method compares output against hard coded values
    int TestupdateHeight()
    {
        int case_marks = 0;
        //ADDED by t1hussai
        boolean normalized = true;
        boolean sequivCheck = true;
        boolean gequivCheck = true;
        boolean retval;
        try
        {
        System.out.println("-----------------------------------");
        System.out.println("Testing updateHeight() method...");
        System.out.println("-----------------------------------");

        //RU
        retval = a2.updateHeight(860,10);
        System.out.println("Return Value: " + retval);
        System.out.println("Correct Value: " + true);

        if ( retval == true )
            case_marks += 2;

        // verify from database directly
        Statement stmt = null;
        ResultSet rs = null;
        int dept_count = 0;
        int count = 0;
        String queryString;
        int height = 0;
        
        // verify total no of rows and newly inserted from department table
        stmt = conn.createStatement();
        queryString = "select height from a2.country where cid = 860";
        rs = stmt.executeQuery(queryString);
        
        if ( rs.next() )
            height = rs.getInt(1);
        System.out.println("\nVerify the value in database");
        System.out.println("Return values:" + height);
        System.out.println("Correct Values: " + 4633);

        if(height == 4633)
            case_marks += 3;
        }

        catch (SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                    "<Message>: " + se.getMessage());
            return case_marks;
        }

        return case_marks;
    }

    int TestupdateDB()
    {
        int case_marks = 0;
        try{
            System.out.println("-----------------------------------");
            System.out.println("Testing updateDB() method...");
            System.out.println("-----------------------------------");

            boolean result = a2.updateDB();
            
            Statement stmt = null;
            ResultSet rs = null;
            String queryString;
            int cid[] = {76, 156, 356, 484, 643, 840};
            String cname[] = {"Brazil", "China", "India", "Mexico", "Russia", "United States"};

            // verify total no of rows and newly inserted from department table
            System.out.println("Querying mostPopulousCountries...");
            stmt = conn.createStatement();
            queryString = "select * from a2.mostPopulousCountries order by cid ASC";
            rs = stmt.executeQuery(queryString);

            int cmid = -1;
            String cmname = "";
            int count = 0;
            int resultNum = 0;
            System.out.println("Your response:");
            while ( rs.next() )
            {
                resultNum++;
                cmid = rs.getInt(1);    
                cmname = rs.getString(2);
                System.out.println("cid: " + cmid + ", cname:" + cmname);
                if (count != 6 && cmid == cid[count] && cmname.compareTo(cname[count]) == 0) {
                    count++;
                }
            }
            if (result && resultNum == 6) {
                case_marks+=2;
            }
            if (count==6) {
                case_marks+=3; // for getting the correct result
            }
            System.out.println("\nCorrect Response:");
            for (int i = 0; i < cid.length; i++)
                System.out.println("cid: " + cid[i] + ", cname:" + cname[i]);
            System.out.println("\n");

        }catch(SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                "<Message>: " + se.getMessage());
        }
        return case_marks;
    }
    
    
    int TestdisconnectDB()
    {
        int case_marks = 0;
        System.out.println("-----------------------------------");
        System.out.println("Testing disconnectDB() method...");
        System.out.println("-----------------------------------");
        
        boolean success = a2.disconnectDB();
        /*
        boolean success = false;
                try{
            success = a2.disconnectDB();
        }catch(SQLException se)
        {
            System.err.println("SQL Exception in main(). " +
                "<Message>: " + se.getMessage());
        }
        */
        System.out.println("Return Value: " + success);
        System.out.println("Correct Values: " + true);
        
        if ( success == true )
            return case_marks += 5;

        return case_marks;
        
    }

    String getTestStatus(int val, int tot)
    {
            return "" + val + " /" + tot;
    }   
}   
