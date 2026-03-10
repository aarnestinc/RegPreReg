USE [OASIS_Conv]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*-- =============================================
-- Author:		Purvesh Patel
-- Create date: 02/03/2026
-- Modified:	03/10/2026
-- Description:	After loading the data into Regular PreReg for 2k,3k and K
--              it calls the SP [bio].[sp_InsertStudentDetails] to insert the data.
--              Updated to pass all 77 parameters, per-row error handling,
--              proper staging column mapping, @IsPreregister=1, @Student_PersonID=0.
-- Usage:       EXEC [enrollment].[usp_ProcessPreRegistrationBatch] @BatchID = 1
-- =============================================*/

CREATE PROCEDURE [enrollment].[usp_ProcessPreRegistrationBatch]
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare variables mapped from staging table
    DECLARE
        @InputStudentID NVARCHAR(9),
        @StudentFirstName NVARCHAR(50),
        @StudentLastName NVARCHAR(50),
        @Gender NCHAR(1),
        @BirthDate DATE,
        @AddressStreetNumber NVARCHAR(50),
        @AddressStreetName NVARCHAR(50),
        @AddressApartmentNumber NVARCHAR(50),
        @City NVARCHAR(50),
        @Borough NCHAR(1),
        @State NCHAR(2),
        @ZipCode NVARCHAR(10),
        @OfferSchoolDBN NCHAR(6),
        @GradeLevel NCHAR(2),
        @GradeCode NCHAR(3),
        @GuardianLastName NVARCHAR(50),
        @GuardianFirstName NVARCHAR(50),
        @GuardianMiddleInitial NCHAR(1),
        @GuardianPhoneNumber NVARCHAR(50),
        @GuardianEmail NVARCHAR(100),
        @CreatedBy NVARCHAR(100),
        @EnrollmentPreRegistrationBatchId INT;

    -- Cursor: only process active records with NO validation errors
    DECLARE student_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT
        [EnrollmentPreRegistrationBatchId],
        [InputStudentID], [StudentFirstName], [StudentLastName], [Gender],
        [BirthDate], [AddressStreetNumber], [AddressStreetName], [AddressApartmentNumber],
        [City], [Borough], [State], [ZipCode],
        [OfferSchoolDBN], [GradeLevel], [GradeCode],
        [GuardianLastName], [GuardianFirstName], [GuardianMiddleInitial],
        [GuardianPhoneNumber], [GuardianEmail], [CreatedBy]
    FROM [enrollment].[EnrollmentPreRegistrationBatch]
    WHERE [BatchId] = @BatchID
      AND [IsActive] = 1
      AND [ErrorMessage] IS NULL;

    OPEN student_cursor;

    FETCH NEXT FROM student_cursor INTO
        @EnrollmentPreRegistrationBatchId,
        @InputStudentID, @StudentFirstName, @StudentLastName, @Gender,
        @BirthDate, @AddressStreetNumber, @AddressStreetName, @AddressApartmentNumber,
        @City, @Borough, @State, @ZipCode,
        @OfferSchoolDBN, @GradeLevel, @GradeCode,
        @GuardianLastName, @GuardianFirstName, @GuardianMiddleInitial,
        @GuardianPhoneNumber, @GuardianEmail, @CreatedBy;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Call sp_InsertStudentDetails with all 77 parameters
            EXEC [bio].[sp_InsertStudentDetails]
                @StudentID              = @InputStudentID,
                @FirstName              = @StudentFirstName,
                @LastName               = @StudentLastName,
                @MiddleName             = NULL,
                @ChosenFirstName        = NULL,
                @ChosenLastName         = NULL,
                @ChosenMiddleName       = NULL,
                @RefGenderCode          = @Gender,
                @BirthDate              = @BirthDate,
                @SchoolDBN              = @OfferSchoolDBN,
                @GradeLevelID           = @GradeLevel,
                @GradeCode              = @GradeCode,
                @AdmissionDate          = '',
                @AdmissionCode          = '',
                @AdmissionReason        = '',
                @OfficialClass          = '',
                @RefProofAddressCode    = NULL,
                @RefProofofAddress1Code = NULL,
                @CertificateNumber      = NULL,
                @RefPlaceofBirthCode    = NULL,
                @StreetNumber           = @AddressStreetNumber,
                @HouseNumber            = @AddressApartmentNumber,
                @City                   = @City,
                @RefStateCode           = @State,
                @ZipCode                = @ZipCode,
                @MSIRefBoroCode         = @Borough,
                @MSIRefDistrictCode     = NULL,
                @GeographicalCode       = NULL,
                @IsHispanicOrLatino     = NULL,
                @HomeLanguageCode       = NULL,
                @WrittenLanguageCode    = NULL,
                @SpokenLanguageCode     = NULL,
                @PhoneNumber            = @GuardianPhoneNumber,
                @WorkPhone              = NULL,
                @CellPhone              = NULL,
                @SeatType               = NULL,
                @HealthInsuranceCode    = NULL,
                @HealthAlertCode        = NULL,
                @IEP                    = NULL,
                @UNAC                   = NULL,
                @ProofDocUpload         = NULL,
                @SiteDistrict           = NULL,
                @SiteID                 = NULL,
                @GuardianFirstName      = @GuardianFirstName,
                @GuardianLastName       = @GuardianLastName,
                @GuardianMiddleName     = @GuardianMiddleInitial,
                @GuardianEmailAddress   = @GuardianEmail,
                @GuardianStreetNumber   = NULL,
                @GuardianHouseNumber    = NULL,
                @GuardianAptNumber      = NULL,
                @GuardianCity           = NULL,
                @GuardianRefStateCode   = NULL,
                @GuardianZipCode        = NULL,
                @GuardianMSIRefBoroCode = NULL,
                @RefRelationshipTypeCode    = NULL,
                @RefAuthorizationCodeCode   = NULL,
                @EmailAddress           = NULL,
                @HousingStatusCode      = NULL,
                @ResidesWithStudentCode = NULL,
                @CreatedBy              = @CreatedBy,
                @RefPersonalInformationVerificationCode = '',
                @OutsideNewYorkCity     = 0,
                @Student_PersonID       = 0,          -- New student, let SP create PersonID
                @ProviderOrganizationID = 0,          -- SP derives from SchoolDBN when 0
                @Ethnicity              = '',
                @StreetName             = @AddressStreetName,
                @BypassDuplicateCheck   = 0,          -- Enable duplicate checking
                @IsPreregister          = 1,          -- BRD requirement: must be true
                @RefNonResidentTuitionCode = NULL,
                @WasHLISCompleted       = 0,
                @WasStudentRecordReceived  = 0,
                @NYSID                  = NULL,
                @RefAddressChangeCode   = NULL,
                @PreRegistrationAnswerToQuestion1Code = NULL,
                @PreRegistrationAnswerToQuestion2Code = NULL;

        END TRY
        BEGIN CATCH
            -- Per-row error handling: log error to staging table, continue processing
            UPDATE [enrollment].[EnrollmentPreRegistrationBatch]
            SET [ErrorMessage] = 'Processing Error: ' + ERROR_MESSAGE()
            WHERE [EnrollmentPreRegistrationBatchId] = @EnrollmentPreRegistrationBatchId;
        END CATCH

        FETCH NEXT FROM student_cursor INTO
            @EnrollmentPreRegistrationBatchId,
            @InputStudentID, @StudentFirstName, @StudentLastName, @Gender,
            @BirthDate, @AddressStreetNumber, @AddressStreetName, @AddressApartmentNumber,
            @City, @Borough, @State, @ZipCode,
            @OfferSchoolDBN, @GradeLevel, @GradeCode,
            @GuardianLastName, @GuardianFirstName, @GuardianMiddleInitial,
            @GuardianPhoneNumber, @GuardianEmail, @CreatedBy;
    END

    CLOSE student_cursor;
    DEALLOCATE student_cursor;

    -- Output summary
    DECLARE @TotalProcessed INT, @TotalErrors INT;
    SELECT @TotalProcessed = COUNT(*) FROM [enrollment].[EnrollmentPreRegistrationBatch]
        WHERE [BatchId] = @BatchID AND [IsActive] = 1 AND [ErrorMessage] IS NULL;
    SELECT @TotalErrors = COUNT(*) FROM [enrollment].[EnrollmentPreRegistrationBatch]
        WHERE [BatchId] = @BatchID AND [IsActive] = 1 AND [ErrorMessage] IS NOT NULL;

    PRINT 'Batch ' + CAST(@BatchID AS VARCHAR(10)) + ' completed. Processed: ' + CAST(@TotalProcessed AS VARCHAR(10)) + ', Errors: ' + CAST(@TotalErrors AS VARCHAR(10));
END
GO
