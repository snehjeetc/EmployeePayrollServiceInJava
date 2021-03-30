package com.employeepayroll.ermodel;

import com.employeepayroll.EmployeePayrollData;
import com.employeepayroll.EmployeePayrollService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ERModelDBServiceTest {
    private ERModelService erModelService;

    @Before
    public void init(){
        erModelService = new ERModelService();
    }

    @Test
    public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() throws ERModelExceptions {
        List<EmployeePayrollData> employeePayrollDataList = erModelService.readData();
        Assert.assertEquals(4, employeePayrollDataList.size());
    }

    @Test
    public void givenNewSalaryForEmployee_WhenUpdated_UsingStatement_ShouldSyncWithDatabase() throws ERModelExceptions {
        List<EmployeePayrollData> employeePayrollDataList =
                erModelService.readData();
        erModelService.updateEmployeeSalary("Bill", 300000);
        boolean isSync = erModelService.checkEmployeePayrollInSyncWithDB("Bill");
        Assert.assertTrue(isSync);
    }
}
