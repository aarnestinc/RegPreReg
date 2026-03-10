DECLARE @BatchID INT = 1; -- Target Batch ID

-- Declare variables for mapping
DECLARE 
    @StudentID NVARCHAR(40), @FirstName NVARCHAR(75), @LastName NVARCHAR(75),
    @Gender NVARCHAR(50), @BirthDate DATE, @SchoolDBN VARCHAR(10),
    @GradeLevelID VARCHAR(10), @GradeCode VARCHAR(50), @CreatedBy VARCHAR(250),
    @Student_PersonID INT, @OfficialClass VARCHAR(10), @StreetName VARCHAR(100),
    @City NVARCHAR(70), @ZipCode NVARCHAR(17), @GuardianFirstName NVARCHAR(75),
    @GuardianLastName NVARCHAR(75), @GuardianEmail NVARCHAR(128);

-- Cursor to loop through the staging table
DECLARE student_cursor CURSOR FOR 
SELECT 
    [StudentId], [StudentFirstName], [StudentLastName], [Gender], 
    [BirthDate], [OfferSchoolDBN], [GradeLevel], [GradeCode], 
    [CreatedBy], [ResolvedStudentId], [OfficialClassId], [AddressStreetName],
    [City], [ZipCode], [GuardianFirstName], [GuardianLastName], [GuardianEmail]
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE [BatchId] = @BatchID and ErrorMessage IS NOT NULL;;

OPEN student_cursor;

FETCH NEXT FROM student_cursor INTO 
    @StudentID, @FirstName, @LastName, @Gender, 
    @BirthDate, @SchoolDBN, @GradeLevelID, @GradeCode, 
    @CreatedBy, @Student_PersonID, @OfficialClass, @StreetName,
    @City, @ZipCode, @GuardianFirstName, @GuardianLastName, @GuardianEmail;

-- Begin the Transaction
BEGIN TRANSACTION;

BEGIN TRY
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Execute the procedure for the current row
        EXEC [bio].[sp_InsertStudentDetails]
            @StudentID = @StudentID,
            @FirstName = @FirstName,
            @LastName = @LastName,
            @RefGenderCode = @Gender,
            @BirthDate = @BirthDate,
            @SchoolDBN = @SchoolDBN,
            @GradeLevelID = @GradeLevelID,
            @GradeCode = @GradeCode,
            @OfficialClass = @OfficialClass,
            @CreatedBy = @CreatedBy,
            @Student_PersonID = @Student_PersonID,
            -- Required placeholders (Adjust these as needed)
            @AdmissionDate = '2026-09-01',
            @AdmissionCode = '10',
            @AdmissionReason = 'NEW',
            @RefPersonalInformationVerificationCode = 'B',
            @ProviderOrganizationID = 1,
            @Ethnicity = '01',
            -- Optional variables
            @StreetName = @StreetName,
            @City = @City,
            @ZipCode = @ZipCode,
            @GuardianFirstName = @GuardianFirstName,
            @GuardianLastName = @GuardianLastName,
            @GuardianEmailAddress = @GuardianEmail;

        FETCH NEXT FROM student_cursor INTO 
            @StudentID, @FirstName, @LastName, @Gender, 
            @BirthDate, @SchoolDBN, @GradeLevelID, @GradeCode, 
            @CreatedBy, @Student_PersonID, @OfficialClass, @StreetName,
            @City, @ZipCode, @GuardianFirstName, @GuardianLastName, @GuardianEmail;
    END

    -- If we reach here without error, commit all changes
    COMMIT TRANSACTION;
    PRINT 'Batch processed and committed successfully.';
END TRY
BEGIN CATCH
    -- If any error occurs, roll back all changes made in this transaction
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Output error details
    PRINT 'Transaction failed and was rolled back.';
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
END CATCH

CLOSE student_cursor;
DEALLOCATE student_cursor;