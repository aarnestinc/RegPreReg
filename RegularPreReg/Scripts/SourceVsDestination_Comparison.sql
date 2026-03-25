/*===========================================================================
  Source vs Destination Comparison
  Purpose : Side-by-side comparison of staging (source) data against all
            destination tables populated by [bio].[sp_InsertStudentDetails].
  Database: Run on OASIS_Conv (cross-DB join to OASIS)
  Usage   : Set @BatchID. Optionally set @InputStudentID to filter one student.
  Author  : Purvesh Patel
  Created : 2026-03-13
===========================================================================*/

USE [OASIS_Conv]
GO

DECLARE @BatchID        INT          = 1;        -- Target batch
DECLARE @InputStudentID NVARCHAR(9)  = NULL;     -- NULL = all students, or set a specific ID

/*---------------------------------------------------------------------------
  SECTION 1: Summary Counts — Source vs Each Destination Table
  Quick row-count comparison across all tables touched by the SP.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 1: Row Count — Source vs Destination Tables =====';

SELECT 'SOURCE: Staging (Passed Validation)' AS [Table], COUNT(*) AS [RowCount]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
WHERE stg.BatchId = @BatchID
  AND stg.IsActive = 1
  AND stg.ErrorMessage IS NULL

UNION ALL
SELECT 'SOURCE: Staging (Processed - StudentId Set)', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
WHERE stg.BatchId = @BatchID
  AND stg.StudentId IS NOT NULL

UNION ALL
SELECT 'DEST: bio.Student', COUNT(DISTINCT s.StudentId)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: enrollment.Enrollment', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[enrollment].[Enrollment] e ON e.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: bio.StudentAddress', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[bio].[StudentAddress] sa ON sa.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: bio.Guardian (via Relationship)', COUNT(DISTINCT g.GuardianId)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[bio].[StudentGuardianRelationship] sgr ON sgr.StudentRecordId = s.StudentRecordId
JOIN [OASIS].[bio].[Guardian] g ON g.GuardianId = sgr.GuardianId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: bio.GuardianAddress', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[bio].[StudentGuardianRelationship] sgr ON sgr.StudentRecordId = s.StudentRecordId
JOIN [OASIS].[bio].[GuardianAddress] ga ON ga.GuardianId = sgr.GuardianId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: bio.StudentGuardianRelationship', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[bio].[StudentGuardianRelationship] sgr ON sgr.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'DEST: bio.StudentEthnicity', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
JOIN [OASIS].[bio].[StudentEthnicity] se ON se.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID

UNION ALL
SELECT 'SOURCE: Staging (Failed Validation)', COUNT(*)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
WHERE stg.BatchId = @BatchID
  AND stg.ErrorMessage IS NOT NULL;


/*---------------------------------------------------------------------------
  SECTION 2: Student — Source vs Destination (bio.Student)
  Side-by-side: staging values vs what landed in OASIS.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 2: Student Detail — Source vs bio.Student =====';

SELECT
    stg.InputStudentID,
    -- SOURCE (Staging)
    stg.StudentFirstName    AS [SRC_FirstName],
    stg.StudentLastName     AS [SRC_LastName],
    stg.Gender              AS [SRC_Gender],
    stg.BirthDate           AS [SRC_BirthDate],
    -- DESTINATION (OASIS)
    s.StudentId             AS [DST_StudentId],
    s.FirstName             AS [DST_FirstName],
    s.LastName              AS [DST_LastName],
    g.GenderCode            AS [DST_Gender],
    s.BirthDate             AS [DST_BirthDate],
    -- MATCH CHECK
    CASE WHEN s.StudentId IS NULL THEN 'MISSING'
         WHEN stg.StudentFirstName = s.FirstName
          AND stg.StudentLastName  = s.LastName
          AND stg.BirthDate        = s.BirthDate
         THEN 'MATCH' ELSE 'MISMATCH'
    END                     AS [StudentCheck]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[ref].[RefGender] g ON g.GenderId = s.GenderId
WHERE stg.BatchId = @BatchID
  AND stg.IsActive = 1
  AND stg.ErrorMessage IS NULL
  AND (@InputStudentID IS NULL OR stg.InputStudentID = @InputStudentID)
ORDER BY stg.StudentLastName, stg.StudentFirstName;


/*---------------------------------------------------------------------------
  SECTION 3: Enrollment — Source vs Destination (enrollment.Enrollment)
  School, grade, and enrollment status comparison.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 3: Enrollment — Source vs enrollment.Enrollment =====';

SELECT
    stg.InputStudentID,
    stg.StudentLastName + ', ' + stg.StudentFirstName AS [StudentName],
    -- SOURCE
    stg.OfferSchoolDBN      AS [SRC_SchoolDBN],
    stg.GradeCode           AS [SRC_GradeCode],
    stg.GradeLevel          AS [SRC_GradeLevel],
    -- DESTINATION
    e.SchoolDBN             AS [DST_SchoolDBN],
    gl.GradeLevelCode       AS [DST_GradeLevel],
    gc.GradeCode            AS [DST_GradeCode],
    es.EnrollmentStatusCode AS [DST_EnrollStatus],
    e.AdmissionDate         AS [DST_AdmissionDate],
    -- MATCH CHECK
    CASE WHEN e.EnrollmentId IS NULL THEN 'MISSING'
         WHEN stg.OfferSchoolDBN = e.SchoolDBN THEN 'MATCH'
         ELSE 'MISMATCH'
    END                     AS [SchoolDBNCheck]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[enrollment].[Enrollment] e ON e.StudentRecordId = s.StudentRecordId
LEFT JOIN [OASIS].[ref].[RefEnrollmentStatus] es ON es.EnrollmentStatusId = e.EnrollmentStatusId
LEFT JOIN [OASIS].[ref].[RefGradeLevel] gl ON gl.GradeLevelId = e.GradeLevelId
LEFT JOIN [OASIS].[ref].[RefGradeCode] gc ON gc.GradeCodeId = e.GradeCodeId
WHERE stg.BatchId = @BatchID
  AND stg.IsActive = 1
  AND stg.ErrorMessage IS NULL
  AND (@InputStudentID IS NULL OR stg.InputStudentID = @InputStudentID)
ORDER BY stg.StudentLastName, stg.StudentFirstName;


/*---------------------------------------------------------------------------
  SECTION 4: Address — Source vs Destination (bio.StudentAddress)
---------------------------------------------------------------------------*/
PRINT '===== SECTION 4: Address — Source vs bio.StudentAddress =====';

SELECT
    stg.InputStudentID,
    stg.StudentLastName + ', ' + stg.StudentFirstName AS [StudentName],
    -- SOURCE
    stg.AddressStreetNumber AS [SRC_StreetNum],
    stg.AddressStreetName   AS [SRC_StreetName],
    stg.AddressApartmentNumber AS [SRC_Apt],
    stg.City                AS [SRC_City],
    stg.State               AS [SRC_State],
    stg.ZipCode             AS [SRC_Zip],
    stg.Borough             AS [SRC_Borough],
    -- DESTINATION
    sa.AddressStreetNumber  AS [DST_StreetNum],
    sa.AddressStreetName    AS [DST_StreetName],
    sa.AddressApartmentNumber AS [DST_Apt],
    sa.City                 AS [DST_City],
    st.StateCode            AS [DST_State],
    sa.ZipCode              AS [DST_Zip],
    b.BoroughCode           AS [DST_Borough],
    -- MATCH CHECK
    CASE WHEN sa.StudentAddressId IS NULL THEN 'MISSING'
         WHEN ISNULL(stg.AddressStreetNumber,'') = ISNULL(sa.AddressStreetNumber,'')
          AND ISNULL(stg.AddressStreetName,'')   = ISNULL(sa.AddressStreetName,'')
          AND ISNULL(stg.City,'')                = ISNULL(sa.City,'')
          AND ISNULL(stg.ZipCode,'')             = ISNULL(sa.ZipCode,'')
         THEN 'MATCH' ELSE 'MISMATCH'
    END                     AS [AddressCheck]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[bio].[StudentAddress] sa ON sa.StudentRecordId = s.StudentRecordId
LEFT JOIN [OASIS].[ref].[RefState] st ON st.StateId = sa.StateId
LEFT JOIN [OASIS].[ref].[RefBorough] b ON b.BoroughId = sa.BoroughId
WHERE stg.BatchId = @BatchID
  AND stg.IsActive = 1
  AND stg.ErrorMessage IS NULL
  AND (@InputStudentID IS NULL OR stg.InputStudentID = @InputStudentID)
ORDER BY stg.StudentLastName, stg.StudentFirstName;


/*---------------------------------------------------------------------------
  SECTION 5: Guardian — Source vs Destination (bio.Guardian)
---------------------------------------------------------------------------*/
PRINT '===== SECTION 5: Guardian — Source vs bio.Guardian =====';

SELECT
    stg.InputStudentID,
    stg.StudentLastName + ', ' + stg.StudentFirstName AS [StudentName],
    -- SOURCE
    stg.GuardianFirstName   AS [SRC_GuardianFirst],
    stg.GuardianLastName    AS [SRC_GuardianLast],
    stg.GuardianMiddleInitial AS [SRC_GuardianMI],
    stg.GuardianPhoneNumber AS [SRC_Phone],
    stg.GuardianEmail       AS [SRC_Email],
    -- DESTINATION
    gd.FirstName            AS [DST_GuardianFirst],
    gd.LastName             AS [DST_GuardianLast],
    gd.MiddleName           AS [DST_GuardianMiddle],
    gd.PrimaryPhoneNumber   AS [DST_Phone],
    sgr.IsPrimaryGuardian   AS [DST_IsPrimary],
    -- MATCH CHECK
    CASE WHEN gd.GuardianId IS NULL THEN 'MISSING'
         WHEN ISNULL(stg.GuardianFirstName,'') = ISNULL(gd.FirstName,'')
          AND ISNULL(stg.GuardianLastName,'')  = ISNULL(gd.LastName,'')
         THEN 'MATCH' ELSE 'MISMATCH'
    END                     AS [GuardianCheck]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[bio].[StudentGuardianRelationship] sgr ON sgr.StudentRecordId = s.StudentRecordId
LEFT JOIN [OASIS].[bio].[Guardian] gd ON gd.GuardianId = sgr.GuardianId
WHERE stg.BatchId = @BatchID
  AND stg.IsActive = 1
  AND stg.ErrorMessage IS NULL
  AND (@InputStudentID IS NULL OR stg.InputStudentID = @InputStudentID)
ORDER BY stg.StudentLastName, stg.StudentFirstName;


/*---------------------------------------------------------------------------
  SECTION 6: Mismatch Summary — Quick count of issues per table
  Shows how many rows matched, mismatched, or are missing per destination.
---------------------------------------------------------------------------*/
PRINT '===== SECTION 6: Mismatch Summary =====';

-- Student mismatches
SELECT 'bio.Student' AS [Table],
    SUM(CASE WHEN s.StudentId IS NULL THEN 1 ELSE 0 END) AS [Missing],
    SUM(CASE WHEN s.StudentId IS NOT NULL
              AND stg.StudentFirstName = s.FirstName
              AND stg.StudentLastName  = s.LastName
              AND stg.BirthDate        = s.BirthDate
         THEN 1 ELSE 0 END) AS [Match],
    SUM(CASE WHEN s.StudentId IS NOT NULL
              AND (stg.StudentFirstName != s.FirstName
               OR  stg.StudentLastName  != s.LastName
               OR  stg.BirthDate        != s.BirthDate)
         THEN 1 ELSE 0 END) AS [Mismatch]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
WHERE stg.BatchId = @BatchID AND stg.IsActive = 1 AND stg.ErrorMessage IS NULL

UNION ALL

-- Enrollment mismatches
SELECT 'enrollment.Enrollment',
    SUM(CASE WHEN e.EnrollmentId IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN e.EnrollmentId IS NOT NULL AND stg.OfferSchoolDBN = e.SchoolDBN THEN 1 ELSE 0 END),
    SUM(CASE WHEN e.EnrollmentId IS NOT NULL AND stg.OfferSchoolDBN != e.SchoolDBN THEN 1 ELSE 0 END)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[enrollment].[Enrollment] e ON e.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID AND stg.IsActive = 1 AND stg.ErrorMessage IS NULL

UNION ALL

-- Address mismatches
SELECT 'bio.StudentAddress',
    SUM(CASE WHEN sa.StudentAddressId IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN sa.StudentAddressId IS NOT NULL
              AND ISNULL(stg.AddressStreetNumber,'') = ISNULL(sa.AddressStreetNumber,'')
              AND ISNULL(stg.City,'') = ISNULL(sa.City,'')
         THEN 1 ELSE 0 END),
    SUM(CASE WHEN sa.StudentAddressId IS NOT NULL
              AND (ISNULL(stg.AddressStreetNumber,'') != ISNULL(sa.AddressStreetNumber,'')
               OR  ISNULL(stg.City,'') != ISNULL(sa.City,''))
         THEN 1 ELSE 0 END)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[bio].[StudentAddress] sa ON sa.StudentRecordId = s.StudentRecordId
WHERE stg.BatchId = @BatchID AND stg.IsActive = 1 AND stg.ErrorMessage IS NULL

UNION ALL

-- Guardian mismatches
SELECT 'bio.Guardian',
    SUM(CASE WHEN gd.GuardianId IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN gd.GuardianId IS NOT NULL
              AND ISNULL(stg.GuardianFirstName,'') = ISNULL(gd.FirstName,'')
              AND ISNULL(stg.GuardianLastName,'')  = ISNULL(gd.LastName,'')
         THEN 1 ELSE 0 END),
    SUM(CASE WHEN gd.GuardianId IS NOT NULL
              AND (ISNULL(stg.GuardianFirstName,'') != ISNULL(gd.FirstName,'')
               OR  ISNULL(stg.GuardianLastName,'')  != ISNULL(gd.LastName,''))
         THEN 1 ELSE 0 END)
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch] stg
LEFT JOIN [OASIS].[bio].[Student] s ON s.StudentId = stg.StudentId
LEFT JOIN [OASIS].[bio].[StudentGuardianRelationship] sgr ON sgr.StudentRecordId = s.StudentRecordId
LEFT JOIN [OASIS].[bio].[Guardian] gd ON gd.GuardianId = sgr.GuardianId
WHERE stg.BatchId = @BatchID AND stg.IsActive = 1 AND stg.ErrorMessage IS NULL;

GO
