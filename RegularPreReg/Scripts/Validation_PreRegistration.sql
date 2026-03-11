/*===========================================================================
  Validation Script - Regular Pre-Registration
  Purpose : End-to-end validation between staging (OASIS_Conv) and
            destination (OASIS).  Run AFTER the SSIS package completes.
  Author  : Purvesh Patel
  Updated : 2026-03-11
  Usage   : Change @BatchID to the batch you want to validate.
            Replace the sample StudentId values with real IDs from your batch.
===========================================================================*/

USE [OASIS_Conv]
GO

-- *** SET YOUR BATCH ID HERE ***
DECLARE @BatchID INT = 1;   -- <-- change to target batch


/*---------------------------------------------------------------------------
  SECTION 1: Staging Batch Summary
  How many records loaded, how many passed/failed validation, how many
  were processed successfully.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 1: Staging Batch Summary =====';

SELECT
    @BatchID                                            AS BatchID,
    COUNT(*)                                            AS TotalRows,
    SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END)      AS ActiveRows,
    SUM(CASE WHEN IsActive = 0 THEN 1 ELSE 0 END)      AS InactiveRows,
    SUM(CASE WHEN ErrorMessage IS NULL AND IsActive = 1 THEN 1 ELSE 0 END) AS PassedValidation,
    SUM(CASE WHEN ErrorMessage IS NOT NULL             THEN 1 ELSE 0 END) AS FailedValidation,
    SUM(CASE WHEN StudentId IS NOT NULL                THEN 1 ELSE 0 END) AS ProcessedByLogicSP,
    MIN(CreatedDate)                                    AS BatchLoadStart,
    MAX(CreatedDate)                                    AS BatchLoadEnd,
    MIN(BatchType)                                      AS BatchType
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID;


/*---------------------------------------------------------------------------
  SECTION 2: Validation Errors Breakdown
  List all distinct error messages and their counts for the batch.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 2: Validation Error Breakdown =====';

SELECT
    ErrorMessage,
    COUNT(*)  AS RowCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId   = @BatchID
  AND ErrorMessage IS NOT NULL
GROUP BY ErrorMessage
ORDER BY RowCount DESC;


/*---------------------------------------------------------------------------
  SECTION 3: Source vs Destination Row Count
  Compare staging count (passed validation) vs records inserted into OASIS.
  NOTE: bio.Student.StudentId is populated back to staging by
        usp_ProcessPreRegistrationBatch after each insert.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 3: Source vs Destination Count =====';

SELECT
    'Staging - Passed Validation'   AS [Level],
    COUNT(*)                        AS RowCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID
  AND IsActive = 1
  AND ErrorMessage IS NULL

UNION ALL

SELECT
    'Staging - StudentId Populated (Processed)',
    COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId   = @BatchID
  AND StudentId IS NOT NULL

UNION ALL

SELECT
    'OASIS - bio.Student (created by this batch)',
    COUNT(DISTINCT s.StudentId)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
WHERE stg.BatchId = @BatchID

UNION ALL

SELECT
    'OASIS - enrollment.Enrollment (status PDR)',
    COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student]          s ON s.StudentId = stg.StudentId
JOIN [OASIS].[enrollment].[Enrollment] e ON e.StudentRecordId = s.StudentRecordId
JOIN [OASIS].[ref].[RefEnrollmentStatus] rs ON rs.EnrollmentStatusId = e.EnrollmentStatusId
WHERE stg.BatchId = @BatchID
  AND rs.Code = 'PDR';


/*---------------------------------------------------------------------------
  SECTION 4: Ad-Hoc Student Spot-Check (Source → Staging → Destination)
  Replace the InputStudentID values below with real IDs from your source file.
  This traces a record from staging through to bio.Student and enrollment.Enrollment.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 4: Ad-Hoc Student Spot-Check =====';

-- *** REPLACE THESE WITH REAL osepogeneratedid VALUES FROM YOUR SOURCE FILE ***
DECLARE @SampleIDs TABLE (InputStudentID NVARCHAR(9));
INSERT INTO @SampleIDs VALUES
    ('123456789'),   -- <-- replace
    ('987654321'),   -- <-- replace
    ('111222333');   -- <-- replace

SELECT
    stg.InputStudentID,
    stg.StudentId          AS [Staging_StudentId_AfterProcess],
    stg.StudentLastName,
    stg.StudentFirstName,
    stg.BirthDate,
    stg.Gender,
    stg.OfferSchoolDBN,
    stg.GradeCode,
    stg.GradeLevel,
    stg.IsActive,
    stg.ErrorMessage,
    stg.CreatedBy,
    stg.CreatedDate,
    -- OASIS destination values
    s.StudentId            AS [OASIS_StudentId],
    s.FirstName            AS [OASIS_FirstName],
    s.LastName             AS [OASIS_LastName],
    s.BirthDate            AS [OASIS_BirthDate],
    e.AdmissionDate,
    e.SchoolDBN,
    rs.Code                AS [EnrollmentStatusCode]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN @SampleIDs sid ON sid.InputStudentID = stg.InputStudentID
LEFT JOIN [OASIS].[bio].[Student] s
    ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[enrollment].[Enrollment] e
    ON e.StudentRecordId = s.StudentRecordId
LEFT JOIN [OASIS].[ref].[RefEnrollmentStatus] rs
    ON rs.EnrollmentStatusId = e.EnrollmentStatusId
WHERE stg.BatchId = @BatchID
  AND stg.InputStudentID IN (SELECT InputStudentID FROM @SampleIDs)
ORDER BY stg.InputStudentID;


/*---------------------------------------------------------------------------
  SECTION 5: Critical Column Mapping Checks
  Verify that required columns are populated correctly in staging.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 5: Critical Column Mapping Checks =====';

SELECT
    'IsActive populated'        AS CheckName,
    COUNT(CASE WHEN IsActive IS NULL THEN 1 END) AS NullCount,
    COUNT(CASE WHEN IsActive = 1     THEN 1 END) AS TrueCount,
    COUNT(CASE WHEN IsActive = 0     THEN 1 END) AS FalseCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID

UNION ALL SELECT
    'CreatedBy populated',
    COUNT(CASE WHEN CreatedBy IS NULL THEN 1 END),
    COUNT(CASE WHEN CreatedBy = 'SSIS_PreReg' THEN 1 END),
    COUNT(CASE WHEN CreatedBy != 'SSIS_PreReg' AND CreatedBy IS NOT NULL THEN 1 END)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID

UNION ALL SELECT
    'CreatedDate populated',
    COUNT(CASE WHEN CreatedDate IS NULL THEN 1 END),
    COUNT(CASE WHEN CreatedDate IS NOT NULL THEN 1 END),
    0
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID

UNION ALL SELECT
    'BatchType populated',
    COUNT(CASE WHEN BatchType IS NULL THEN 1 END),
    COUNT(CASE WHEN BatchType IS NOT NULL THEN 1 END),
    0
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID

UNION ALL SELECT
    'GradeLevel populated',
    COUNT(CASE WHEN GradeLevel IS NULL THEN 1 END),
    COUNT(CASE WHEN GradeLevel IS NOT NULL THEN 1 END),
    0
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID

UNION ALL SELECT
    'InputStudentID populated',
    COUNT(CASE WHEN InputStudentID IS NULL THEN 1 END),
    COUNT(CASE WHEN InputStudentID IS NOT NULL THEN 1 END),
    0
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID;


/*---------------------------------------------------------------------------
  SECTION 6: Error Case Validation
  Simulate validation failure cases to confirm the SP catches them.
  These records should appear in the error list — if they don't,
  the validation SP is not working.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 6: Error Case Validation =====';

-- Records missing FirstName
SELECT 'Missing StudentFirstName' AS ErrorCase, COUNT(*) AS RowCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ISNULL(StudentFirstName,'') = ''
UNION ALL
-- Records missing LastName
SELECT 'Missing StudentLastName', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ISNULL(StudentLastName,'') = ''
UNION ALL
-- Records missing BirthDate
SELECT 'Missing BirthDate', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND BirthDate IS NULL
UNION ALL
-- Records missing Gender
SELECT 'Missing Gender', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ISNULL(Gender,'') = ''
UNION ALL
-- Records missing OfferSchoolDBN
SELECT 'Missing OfferSchoolDBN', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ISNULL(OfferSchoolDBN,'') = ''
UNION ALL
-- Records missing GradeCode
SELECT 'Missing GradeCode', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ISNULL(GradeCode,'') = ''
UNION ALL
-- Records with errors but StudentId populated (should NOT happen — SP skips errors)
SELECT 'ERROR: Has ErrorMsg but still processed', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID AND ErrorMessage IS NOT NULL AND StudentId IS NOT NULL;


/*---------------------------------------------------------------------------
  SECTION 7: OASIS Destination Data Quality Check
  Verify records inserted into OASIS look correct — enrollment status = PDR,
  school DBN matches, grade matches, guardian data present.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 7: OASIS Destination Data Quality =====';

SELECT TOP 20
    stg.InputStudentID,
    stg.StudentId          AS StagingStudentId,
    s.StudentId            AS OASISStudentId,
    s.FirstName,
    s.LastName,
    s.BirthDate,
    e.SchoolDBN,
    e.AdmissionDate,
    rs.Code                AS EnrollmentStatus,
    stg.GradeCode,
    stg.GradeLevel,
    stg.GuardianFirstName,
    stg.GuardianLastName,
    stg.GuardianPhoneNumber
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student]          s  ON s.StudentId = stg.StudentId
JOIN [OASIS].[enrollment].[Enrollment] e  ON e.StudentRecordId = s.StudentRecordId
JOIN [OASIS].[ref].[RefEnrollmentStatus] rs ON rs.EnrollmentStatusId = e.EnrollmentStatusId
WHERE stg.BatchId  = @BatchID
  AND rs.Code      = 'PDR'
ORDER BY stg.StudentLastName, stg.StudentFirstName;


/*---------------------------------------------------------------------------
  SECTION 8: Records in Staging NOT Found in OASIS (Unprocessed Successes)
  After processing, all rows with ErrorMessage IS NULL AND IsActive = 1
  should have a corresponding OASIS student record.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 8: Staging-to-OASIS Gap Check =====';

SELECT
    stg.InputStudentID,
    stg.StudentLastName,
    stg.StudentFirstName,
    stg.BirthDate,
    stg.OfferSchoolDBN,
    stg.ErrorMessage       AS StagingError,
    stg.StudentId          AS StagingStudentId,
    CASE
        WHEN s.StudentId IS NULL THEN 'NOT FOUND in OASIS'
        ELSE 'OK'
    END                    AS OASISStatus
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
WHERE stg.BatchId    = @BatchID
  AND stg.IsActive   = 1
  AND stg.ErrorMessage IS NULL
  AND s.StudentId IS NULL   -- should return ZERO rows if all processed successfully
ORDER BY stg.StudentLastName;


/*---------------------------------------------------------------------------
  SECTION 9: Duplicate Detection in Staging
  Check if the same student (same first, last, DOB) appears more than once
  in the same batch — these will hit the duplicate check in the SP.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 9: Duplicates in Staging =====';

SELECT
    StudentFirstName,
    StudentLastName,
    BirthDate,
    COUNT(*) AS DuplicateCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID
  AND IsActive = 1
GROUP BY StudentFirstName, StudentLastName, BirthDate
HAVING COUNT(*) > 1
ORDER BY DuplicateCount DESC;


/*---------------------------------------------------------------------------
  SECTION 10: Full Row Detail for Failed Records
  Show complete staging row for any record that failed validation or
  failed processing.  Use for manual review.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 10: Full Error Row Detail =====';

SELECT
    EnrollmentPreRegistrationBatchId,
    BatchId,
    InputStudentID,
    StudentLastName,
    StudentFirstName,
    BirthDate,
    Gender,
    OfferSchoolDBN,
    GradeCode,
    GradeLevel,
    GuardianFirstName,
    GuardianLastName,
    GuardianPhoneNumber,
    IsActive,
    ErrorMessage,
    SourceFileName
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = @BatchID
  AND (ErrorMessage IS NOT NULL OR StudentId IS NULL)
ORDER BY EnrollmentPreRegistrationBatchId;

GO
