package com.employeepayroll;

import com.google.gson.Gson;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.employeepayroll.EmployeePayrollService.IOService.DB_IO;
import static com.employeepayroll.EmployeePayrollService.IOService.REST_IO;

public class DBServiceTest {
    private EmployeePayrollService employeePayrollService;

    @Before
    public void init(){
        employeePayrollService = new EmployeePayrollService();
    }

    @Before
    public void setup(){
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = 3000;
    }
    @Test
    public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount(){
        List<EmployeePayrollData> employeePayrollData =
                employeePayrollService.readData(DB_IO);
        Assert.assertEquals(3, employeePayrollData.size());
    }

    @Test
    public void givenNewSalaryForEmployee_WhenUpdated_UsingStatement_ShouldSyncWithDatabase(){
        List<EmployeePayrollData> employeePayrollDataList =
                employeePayrollService.readData(DB_IO);
        employeePayrollService.updateEmployeeSalary("Mark", 300000, 1);
        boolean isSync = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark");
        Assert.assertTrue(isSync);
    }

    @Test
    public void givenNewSalaryForEmployee_WhenUpdated_UsingPreparedStatement_ShouldSyncWithDatabase(){
        List<EmployeePayrollData> employeePayrollDataList =
                employeePayrollService.readData(DB_IO);
        employeePayrollService.updateEmployeeSalary("Mark", 200000, 1);
        boolean isSync = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark");
        Assert.assertTrue(isSync);
    }

    @Test
    public void givenDatesInRange_WhenRetrievedDataBetweenTwoDates_ShouldMatchEmployeeCount(){
        String from = "2019-01-01";
        String to = null;
        List<EmployeePayrollData> employeePayrollDataList =
                employeePayrollService.getEmployeePayrollDataBetweenDates(DB_IO, from, to);
        Assert.assertEquals(2, employeePayrollDataList.size());
    }

    @Test
    public void givenEmployeePayrollInDB_WhenCalculated_SUM_MIN_MAX_AVERAGE_ofSalary_ShouldGiveCorrectOutput(){
        List<String> outputFromDB = employeePayrollService.calculateSumAverageMinMax(DB_IO);
        List<String> expectedOutput = Arrays.asList("600000.0", "200000.0", "100000.0", "300000.0");
        Assert.assertEquals(expectedOutput, outputFromDB);
    }

    @Test
    public void givenEmployeePayrollInDB_WhenCalculated_Sum_Min_Max_Average_ofSalary_GroupByGender_ShouldGiveCorrectOutput(){
        Map<String, List<Double>> outputMapFromDB = employeePayrollService.calculateSumAverageMinMax_GroupByGender(DB_IO);
        List<Double> expectedMaleList = Arrays.asList( 300000.00, 150000.00, 100000.00, 200000.00);
        List<Double> expectedFemaleList = Arrays.asList(300000.00, 300000.00, 300000.00, 300000.00);
        Assert.assertTrue(outputMapFromDB.get("M").equals(expectedMaleList) &&
                outputMapFromDB.get("F").equals(expectedFemaleList));
    }

    @Test
    public void givenNewEmployee_WhenAdded_ShouldSyncWithDB(){
        employeePayrollService.readData(DB_IO);
        employeePayrollService.addEmployeeToPayroll("Terissa", 500000.00, LocalDate.now(), "F");
        boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terissa");
        Assert.assertTrue(result);
    }

    @Test
    public void given6Employees_WhenAddedToDB_ShouldMatchEmployeeEntries(){
        EmployeePayrollData[] arrayOfEmps = {
                new EmployeePayrollData(0, "Jeff Bezos", "M", 100000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Bill Gates", "M", 200000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Mark Zuckerberg", "M", 300000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Sunder", "M", 600000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Mukesh", "M", 1000000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Anil", "M", 200000.0, LocalDate.now())
        };
        employeePayrollService.readData(DB_IO);
        Instant start = Instant.now();
        employeePayrollService.addEmployeesToPayroll(Arrays.asList(arrayOfEmps));
        Instant end = Instant.now();
        System.out.println("Duration without thread: " + Duration.between(start, end));
        Instant threadStart = Instant.now();
        employeePayrollService.addEmployeesToPayrollWithThreads(Arrays.asList(arrayOfEmps));
        Instant threadEnd = Instant.now();
        System.out.println("Duration with thread: " + Duration.between(threadStart, threadEnd));
        employeePayrollService.printData(DB_IO);
        Assert.assertEquals(13, employeePayrollService.countEntries(DB_IO));
    }

    @Test
    public void given6EmployeesWhenAdded_ToDB_Using_Multithreading_InDBServiceTestclass_shouldMatchEmployeeEntries(){
        EmployeePayrollData[] arrayOfEmps = {
                new EmployeePayrollData(0, "Tom", "M", 100000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Deep", "M", 200000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Harry", "M", 300000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Ramesh", "M", 600000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Mukesh", "M", 1000000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Suresh", "M", 200000.0, LocalDate.now())
        };
        employeePayrollService.readData(DB_IO);
        employeePayrollService.addEmployeeToPayroll_UsingDBThreads(Arrays.asList(arrayOfEmps));
        long count = employeePayrollService.countEntries(DB_IO);
        Assert.assertEquals(7, count);
    }

    @Test
    public void when6EmployeesAreUpdated_ToDB_Using_Mulit_Threading_ShouldSyncWithDB(){
        EmployeePayrollData[] arrayOfEmps = {
                new EmployeePayrollData(0, "Tom", "M", 12122, LocalDate.now()),
                new EmployeePayrollData(0, "Deep", "M", 22222.0, LocalDate.now()),
                new EmployeePayrollData(0, "Harry", "M", 5555.0, LocalDate.now()),
                new EmployeePayrollData(0, "Ramesh", "M", 88888.0, LocalDate.now()),
                new EmployeePayrollData(0, "Mukesh", "M", 99999.0, LocalDate.now()),
                new EmployeePayrollData(0, "Suresh", "M", 45234.0, LocalDate.now())
        };
        employeePayrollService.readData(DB_IO);
        employeePayrollService.updateEmployees(Arrays.asList(arrayOfEmps));
        boolean result = employeePayrollService.isSyncWithDB(Arrays.asList(arrayOfEmps));
        employeePayrollService.printData(DB_IO);
        Assert.assertTrue(result);
    }

    @Test
    public void givenEmployeeDataInJSONServer_WhenRetrieved_ShouldMatchTheCount(){

        EmployeePayrollData[] arrayOfEmps = getEmployeeList();
        EmployeePayrollService employeePayrollService_new = new EmployeePayrollService(Arrays.asList(arrayOfEmps));
        long entries = employeePayrollService_new.countEntries(REST_IO);
        Assert.assertEquals(2, entries);

    }

    private EmployeePayrollData[] getEmployeeList() {
        Response response = RestAssured.get("/employee_payroll_datas");
        System.out.println("Employee Payroll Entries In JSON Server: \n" + response.asString());
        EmployeePayrollData[] arrayOfEmps = new Gson().fromJson(response.asString(), EmployeePayrollData[].class);
        return arrayOfEmps;
    }
}
