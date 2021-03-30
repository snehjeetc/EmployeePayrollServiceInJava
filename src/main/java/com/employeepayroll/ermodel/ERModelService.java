package com.employeepayroll.ermodel;

import com.employeepayroll.EmployeePayrollData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ERModelService {
    private List<EmployeePayrollData> employeePayrollDataList;
    private Map<Integer, Department> departmentMap;
    private ERModelDBService erModelDBService;

    public ERModelService(){
        this.erModelDBService = ERModelDBService.getInstance();
        employeePayrollDataList = new ArrayList<>();
        departmentMap = new HashMap<>();
    }

    public boolean checkEmployeePayrollInSyncWithDB(String name) throws ERModelExceptions {
        List<EmployeePayrollData> employeePayrollDataList =
                erModelDBService.getEmployeePayrollData(name);
        EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
        if(employeePayrollDataList.size() == 0 && employeePayrollData == null)
            return true;
        else if(employeePayrollData == null)
            return false;
        else if(employeePayrollDataList.size() == 0)
            return false;
        else
            return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
    }


    public List<EmployeePayrollData> readData() throws ERModelExceptions {
        employeePayrollDataList = erModelDBService.readData(departmentMap);
        return employeePayrollDataList;
    }

    public void updateEmployeeSalary(String name, double salary) throws ERModelExceptions {
        int res = erModelDBService.updateEmployeeData(name, salary);
        if(res == 0) return;
        EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
        if(employeePayrollData != null) employeePayrollData.setSalary(salary);
    }

    private EmployeePayrollData getEmployeePayrollData(String name) {
        return this.employeePayrollDataList.stream()
                                           .filter(employeePayrollData -> employeePayrollData.getName().equals(name))
                                           .findFirst()
                                           .orElse(null);
    }

    public List<EmployeePayrollData> getEmployeePayrollDataBetweenDates(String from, String to) throws ERModelExceptions {
        return erModelDBService.getEmployeePayrollDataBetweenDates(from, to);
    }

    public List<Double> calculateSumAverageMinMax() throws ERModelExceptions {
        return erModelDBService.calculateSumAverageMinMax();
    }

    public Map<String, List<Double>> calculateSumAverageMinMax_GroupByGender() throws ERModelExceptions {
        return erModelDBService.calculateSumAverageMinMax_GroupByGender();
    }
}
