{{ config(materialized='table', alias='EmployeeDepartment_cur') }}

SELECT
  emp.BusinessEntityID       AS employee_id,
  emp.LoginID                AS login_id,
  emp.NationalIDNumber       AS national_id,
  emp.JobTitle               AS job_title,
  emp.BirthDate              AS birth_date,
  emp.Gender                 AS gender,
  emp.HireDate               AS hire_date,
  emp.SalariedFlag           AS is_salaried,
  emp.VacationHours          AS vacation_hours,
  emp.SickLeaveHours         AS sick_leave_hours,
  emp.CurrentFlag            AS is_current,
  emp.ModifiedDate           AS employee_modified_date,
  dept.DepartmentID          AS department_id,
  dept.Name                  AS department_name,
  dept.GroupName             AS group_name,
  CURRENT_TIMESTAMP()        AS curation_ingestion_time,
  CURRENT_DATE()             AS row_start_date,
  DATE '9999-12-31'          AS row_end_date
FROM {{ source('Employee_Details_stg', 'Employee_stg') }} emp
JOIN {{ source('Employee_Details_stg', 'Department_stg') }} dept
  ON emp.DepartmentID = dept.DepartmentID
WHERE emp.CurrentFlag = TRUE