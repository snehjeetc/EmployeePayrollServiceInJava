package com.employeepayroll;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.employeepayroll.EmployeePayrollService.IOService.DB_IO;

public class DBServiceTest {
    @Test
    public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount(){
        EmployeePayrollService employeePayrollService =
                new EmployeePayrollService();
        List<EmployeePayrollData> employeePayrollData =
                employeePayrollService.readData(DB_IO);
        Assert.assertEquals(3, employeePayrollData.size());
    }

    @Test
    public void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDatabase(){
        EmployeePayrollService employeePayrollService =
                new EmployeePayrollService();
        List<EmployeePayrollData> employeePayrollDataList =
                employeePayrollService.readData(DB_IO);
        employeePayrollService.updateEmployeeSalary("Mark", 300000);
        boolean isSync = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark");
        Assert.assertTrue(isSync);
    }
}
