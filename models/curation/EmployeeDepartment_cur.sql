{{ config(materialized='incremental', unique_key='BusinessEntityID') }}

WITH staging AS (
  SELECT
    emp.BusinessEntityID       AS BusinessEntityID,
    emp.LoginID                AS LoginID,
    emp.NationalIDNumber       AS NationalIDNumber,
    emp.JobTitle               AS JobTitle,
    emp.BirthDate              AS BirthDate,
    emp.Gender                 AS Gender,
    emp.HireDate               AS HireDate,
    emp.SalariedFlag           AS SalariedFlag,
    emp.VacationHours          AS VacationHours,
    emp.SickLeaveHours         AS SickLeaveHours,
    emp.CurrentFlag            AS CurrentFlag,
    emp.ModifiedDate           AS EmployeeModifiedDate,
    dept.DepartmentID          AS DepartmentID,
    dept.Name                  AS DepartmentName,
    dept.GroupName             AS GroupName,
    CURRENT_TIMESTAMP()        AS CurationIngestionTime,
    CURRENT_DATE()             AS RowStartDate,
    DATE '9999-12-31'          AS RowEndDate,
    {{ dbt_utils.generate_surrogate_key([
      'emp.LoginID', 'emp.NationalIDNumber', 'emp.JobTitle', 'emp.BirthDate',
      'emp.Gender', 'emp.HireDate', 'emp.SalariedFlag', 'emp.VacationHours',
      'emp.SickLeaveHours', 'emp.CurrentFlag', 'emp.ModifiedDate',
      'dept.DepartmentID', 'dept.Name', 'dept.GroupName'
    ]) }} AS RowHash
  FROM {{ source('Employee_Details_stg', 'Employee_stg') }} emp
  JOIN {{ source('Employee_Details_stg', 'Department_stg') }} dept
    ON emp.DepartmentID = dept.DepartmentID
  WHERE emp.CurrentFlag = TRUE
),

latest AS (
  SELECT
    BusinessEntityID,
    LoginID,
    NationalIDNumber,
    JobTitle,
    BirthDate,
    Gender,
    HireDate,
    SalariedFlag,
    VacationHours,
    SickLeaveHours,
    CurrentFlag,
    EmployeeModifiedDate,
    DepartmentID,
    DepartmentName,
    GroupName,
    CurationIngestionTime,
    RowStartDate,
    RowEndDate,
    RowHash
  FROM {{ this }}
  WHERE RowEndDate = DATE '9999-12-31'
),

changes AS (
  SELECT
    s.BusinessEntityID,
    s.LoginID,
    s.NationalIDNumber,
    s.JobTitle,
    s.BirthDate,
    s.Gender,
    s.HireDate,
    s.SalariedFlag,
    s.VacationHours,
    s.SickLeaveHours,
    s.CurrentFlag,
    s.EmployeeModifiedDate,
    s.DepartmentID,
    s.DepartmentName,
    s.GroupName,
    s.CurationIngestionTime,
    s.RowStartDate,
    s.RowEndDate,
    s.RowHash
  FROM staging s
  LEFT JOIN latest l
    ON s.BusinessEntityID = l.BusinessEntityID
  WHERE l.RowHash IS NULL OR s.RowHash != l.RowHash
)

-- Insert new or changed rows
SELECT
  BusinessEntityID,
  LoginID,
  NationalIDNumber,
  JobTitle,
  BirthDate,
  Gender,
  HireDate,
  SalariedFlag,
  VacationHours,
  SickLeaveHours,
  CurrentFlag,
  EmployeeModifiedDate,
  DepartmentID,
  DepartmentName,
  GroupName,
  CurationIngestionTime,
  RowStartDate,
  RowEndDate,
  RowHash
FROM changes

{% if is_incremental() %}
UNION ALL

-- Expire old rows
SELECT
  l.BusinessEntityID,
  l.LoginID,
  l.NationalIDNumber,
  l.JobTitle,
  l.BirthDate,
  l.Gender,
  l.HireDate,
  l.SalariedFlag,
  l.VacationHours,
  l.SickLeaveHours,
  l.CurrentFlag,
  l.EmployeeModifiedDate,
  l.DepartmentID,
  l.DepartmentName,
  l.GroupName,
  l.CurationIngestionTime,
  l.RowStartDate,
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS RowEndDate,
  l.RowHash
FROM latest l
JOIN changes c
  ON l.BusinessEntityID = c.BusinessEntityID
{% endif %}