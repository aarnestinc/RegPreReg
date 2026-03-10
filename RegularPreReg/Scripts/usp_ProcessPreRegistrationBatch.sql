


/*-- =============================================
-- Author:		Purvesh Patel 
-- Create date: 02/03/2026
-- Description:	After loading the data into of Regular PreReg for 2k,3k and K it calls the SP [bio].[sp_InsertStudentDetails] to innsert the data.
[enrollment].[usp_ProcessPreRegistrationBatch] 
-- =============================================*/






CREATE PROCEDURE [enrollment].[usp_ProcessPreRegistrationBatch]
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare variables for row-by-row mapping
    DECLARE 
        @StudentID NVARCHAR(40), @FirstName NVARCHAR(75), @LastName NVARCHAR(75),
        @Gender NVARCHAR(50), @BirthDate DATE, @SchoolDBN VARCHAR(10),
        @GradeLevelID VARCHAR(10), @GradeCode VARCHAR(50), @CreatedBy VARCHAR(250),
        @Student_PersonID INT, @StreetName VARCHAR(100),
        @City NVARCHAR(70), @ZipCode NVARCHAR(17), @GuardianFirstName NVARCHAR(75),
        @GuardianLastName NVARCHAR(75), @GuardianEmail NVARCHAR(128);

    -- Cursor to loop through the staging table based on the provided BatchID
    DECLARE student_cursor CURSOR LOCAL FAST_FORWARD FOR 
    SELECT 
        [inputStudentID], [StudentFirstName], [StudentLastName], [Gender], 
        [BirthDate], [OfferSchoolDBN], [GradeLevel], [GradeCode], 
        [CreatedBy], [ResolvedStudentId],  [AddressStreetName],
        [City], [ZipCode], [GuardianFirstName], [GuardianLastName], [GuardianEmail]
    FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
    WHERE [BatchId] = @BatchID;

    OPEN student_cursor;

    FETCH NEXT FROM student_cursor INTO 
        @StudentID, @FirstName, @LastName, @Gender, 
        @BirthDate, @SchoolDBN, @GradeLevelID, @GradeCode, 
        @CreatedBy, @Student_PersonID,  @StreetName,
        @City, @ZipCode, @GuardianFirstName, @GuardianLastName, @GuardianEmail;

    -- Wrap the entire batch in a transaction
    BEGIN TRANSACTION;

    BEGIN TRY
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Execute the core business logic procedure
            EXEC [bio].[sp_InsertStudentDetails]
                @StudentID = @StudentID,
                @FirstName = @FirstName,
                @LastName = @LastName,
                @RefGenderCode = @Gender,
                @BirthDate = @BirthDate,
                @SchoolDBN = @SchoolDBN,
                @GradeLevelID = @GradeLevelID,
                @GradeCode = @GradeCode,
               @OfficialClass = '',
                @CreatedBy = @CreatedBy,
                @Student_PersonID = @Student_PersonID,
                
                -- Required variables with default logic (Adjust as per business rules)
                @AdmissionDate = '',           
                @AdmissionCode = '',                   
                @AdmissionReason = '',                
                @RefPersonalInformationVerificationCode = '', 
                @ProviderOrganizationID = '',             
                @Ethnicity = '',                       

                -- Optional variables mapped from staging
                @StreetName = @StreetName,
                @City = @City,
                @ZipCode = @ZipCode,
                @GuardianFirstName = @GuardianFirstName,
                @GuardianLastName = @GuardianLastName,
                @GuardianEmailAddress = @GuardianEmail;

            FETCH NEXT FROM student_cursor INTO 
                @StudentID, @FirstName, @LastName, @Gender, 
                @BirthDate, @SchoolDBN, @GradeLevelID, @GradeCode, 
                @CreatedBy, @Student_PersonID,  @StreetName,
                @City, @ZipCode, @GuardianFirstName, @GuardianLastName, @GuardianEmail;
        END

        COMMIT TRANSACTION;
        PRINT 'Batch ' + CAST(@BatchID AS VARCHAR(10)) + ' processed successfully.';
    END TRY
    BEGIN CATCH
        -- Rollback if any single row fails
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise the error back to the calling application
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH

    CLOSE student_cursor;
    DEALLOCATE student_cursor;
END
GO