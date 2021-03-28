package com.employeepayroll;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.employeepayroll.EmployeePayrollService.IOService.DB_IO;

public class DBServiceTest {
    private EmployeePayrollService employeePayrollService;

    @Before
    public void init(){
        employeePayrollService = new EmployeePayrollService();
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
}
