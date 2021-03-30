package com.employeepayroll.ermodel;

import com.employeepayroll.EmployeePayrollData;
import com.employeepayroll.EmployeePayrollService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    @Test
    public void givenDatesInRange_WhenRetrievedDataBetweenTwoDates_ShouldMatchEmployeeCount() throws ERModelExceptions {
        String from = "2019-01-01";
        String to = null;
        List<EmployeePayrollData> employeePayrollDataList =
               erModelService.getEmployeePayrollDataBetweenDates(from, to);
        Assert.assertEquals(2, employeePayrollDataList.size());
    }

    @Test
    public void givenEmployeePayrollInDB_WhenCalculated_SUM_MIN_MAX_AVERAGE_ofSalary_ShouldGiveCorrectOutput() throws ERModelExceptions {
        List<Double> outputFromDB = erModelService.calculateSumAverageMinMax();
        List<Double> expectedOutput = Arrays.asList(410000.0, 102500.0, 30000.0, 300000.0);
        Assert.assertEquals(expectedOutput, outputFromDB);
    }

    @Test
    public void givenEmployeePayrollInDB_WhenCalculated_Sum_Min_Max_Average_ofSalary_GroupByGender_ShouldGiveCorrectOutput() throws ERModelExceptions {
        Map<String, List<Double>> outputMapFromDB = erModelService.calculateSumAverageMinMax_GroupByGender();
        List<Double> expectedMaleList = Arrays.asList( 340000.00, 170000.00, 40000.00, 300000.00);
        List<Double> expectedFemaleList = Arrays.asList(70000.00, 35000.00, 30000.00, 40000.00);
        Assert.assertTrue(outputMapFromDB.get("M").equals(expectedMaleList) &&
                outputMapFromDB.get("F").equals(expectedFemaleList));
    }

    @Test
    public void givenNewEmployee_WhenAdded_ShouldSyncWithDB() throws ERModelExceptions {
        erModelService.readData();
        erModelService.addEmployeeToPayroll("Elizabeth","F", 500000.00, LocalDate.now(),  "R&D", "HR");
        boolean result = erModelService.checkEmployeePayrollInSyncWithDB("Elizabeth");
        Assert.assertTrue(result);
    }

    @Test
    public void givenNewEmployee_WhenRemoved_ShouldRemoveFromSystem_ButNotFromDB() throws ERModelExceptions {
        erModelService.readData();
        erModelService.removeEmployee("Elizabeth");
        boolean result = erModelService.checkEmployeePayrollInSyncWithDB("Elizabeth");
        Assert.assertTrue(result);
    }
}
