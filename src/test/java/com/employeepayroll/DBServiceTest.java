package com.employeepayroll;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
}
