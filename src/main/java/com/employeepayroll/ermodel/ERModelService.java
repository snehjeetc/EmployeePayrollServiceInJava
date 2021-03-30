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

    public List<EmployeePayrollData> readData() throws ERModelExceptions {
        employeePayrollDataList = erModelDBService.readData(departmentMap);
        return employeePayrollDataList;
    }
}
