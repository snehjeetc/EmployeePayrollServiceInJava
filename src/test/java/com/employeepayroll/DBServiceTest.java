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
}
