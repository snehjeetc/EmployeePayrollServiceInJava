package com.employeepayroll;

import com.google.gson.Gson;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Arrays;

import static com.employeepayroll.EmployeePayrollService.IOService.REST_IO;

public class REST_IOServiceTest {
    @Before
    public void setup(){
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = 3000;
    }

    @Test
    public void givenEmployeeDataInJSONServer_WhenRetrieved_ShouldMatchTheCount(){
        EmployeePayrollData[] arrayOfEmps = getEmployeeList();
        EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmps));
        long entries = employeePayrollService.countEntries(REST_IO);
        Assert.assertEquals(2, entries);
    }

    @Test
    public void givenNewEmployee_WhenAdded_ShouldMatch_201ResponseCode_AndTheTotalExpectedCounts(){
        EmployeePayrollData[] arrayOfemps = getEmployeeList();
        EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfemps));
        EmployeePayrollData employeePayrollData = new EmployeePayrollData(0, "Mark Zuckerberg",
                300000.0, LocalDate.now());
        Response response = addEmployeeToJSONServer(employeePayrollData);
        int statusCode = response.getStatusCode();
        Assert.assertEquals(201, statusCode);
        employeePayrollData = new Gson().fromJson(response.asString(), EmployeePayrollData.class);
        employeePayrollService.addEmployeeToPayroll(employeePayrollData, REST_IO);
        long entries = employeePayrollService.countEntries(REST_IO);
        Assert.assertEquals(3, entries);
    }

    @Test
    public void givenList_Of_New_Employees_WhenAdded_ShouldMatch201ResponseCode_AndTheTotalEntries(){
        EmployeePayrollData[] arrayOfemps = getEmployeeList();
        EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfemps));
        EmployeePayrollData[] employeePayrollDatas = new EmployeePayrollData[] {
                new EmployeePayrollData(0, "Ramesh", "M", 34000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Suresh", "M", 40000.0, LocalDate.now()),
                new EmployeePayrollData(0, "Riya", "F", 50000.0, LocalDate.now())
        };
        for(EmployeePayrollData employeePayrollData : employeePayrollDatas){
            Response response = addEmployeeToJSONServer(employeePayrollData);
            Assert.assertEquals(201, response.getStatusCode());
            EmployeePayrollData empData = new Gson().fromJson(response.asString(), EmployeePayrollData.class);
            employeePayrollService.addEmployeeToPayroll(empData, REST_IO);
        }
        long entries = employeePayrollService.countEntries(REST_IO);
        Assert.assertEquals(6, entries);
    }

    @Test
    public void givenNewSalaryForEmployee_WhenUpdated_ShouldMatch200_Response(){
        EmployeePayrollService employeePayrollService;
        EmployeePayrollData[]  arrayOfEmps = getEmployeeList();
        employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmps));
        employeePayrollService.updateEmployeeSalary("Suresh", 234230, REST_IO, null);
        EmployeePayrollData employeePayrollData = employeePayrollService.getEmployeePayrollData("Suresh");

        String empJson = new Gson().toJson(employeePayrollData);
        RequestSpecification request = RestAssured.given();
        request.header("Content-Type", "application/json");
        request.body(empJson);
        Response response = request.put("/employee_payroll_datas/" + employeePayrollData.id);
        int statusCode = response.getStatusCode();
        Assert.assertEquals(200, statusCode);
    }

    @Test
    public void givenEmployeeToDelete_WhenDeleted_ShouldMatch200ResponseCode_and_TheTotalNumberOfEntries(){
        EmployeePayrollService employeePayrollService;
        EmployeePayrollData[] employeePayrollDatas = getEmployeeList();
        employeePayrollService = new EmployeePayrollService(Arrays.asList(employeePayrollDatas));

        EmployeePayrollData employeePayrollData = employeePayrollService.getEmployeePayrollData("Suresh");
        RequestSpecification request = RestAssured.given();
        request.header("Content-Type", "application/json");
        Response response = request.delete("/employee_payroll_datas/"+ employeePayrollData.id);
        int statuscode = response.getStatusCode();
        Assert.assertEquals(200, statuscode);

        employeePayrollService.deleteEmployeePayroll(employeePayrollData.name, REST_IO);
        long entries = employeePayrollService.countEntries(REST_IO);
        Assert.assertEquals(5, entries);
    }

    private Response addEmployeeToJSONServer(EmployeePayrollData employeePayrollData) {
        String empJson = new Gson().toJson(employeePayrollData);
        RequestSpecification request = RestAssured.given();
        request.header("Content-Type", "application/json");
        request.body(empJson);
        return request.post("/employee_payroll_datas");
    }

    private EmployeePayrollData[] getEmployeeList() {
        Response response = RestAssured.get("/employee_payroll_datas");
        System.out.println("Employee Payroll Entries In JSON Server: \n" + response.asString());
        EmployeePayrollData[] arrayOfEmps = new Gson().fromJson(response.asString(), EmployeePayrollData[].class);
        return arrayOfEmps;
    }
}
