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
-- Description:	Validate the business rules for Regular PreReg for 2k,3k and K.
--              Added @BatchID parameter, clear errors before re-validation,
--              added OfferSchoolDBN and GradeCode checks.
-- Usage:       EXEC [enrollment].[usp_ValidateRegularPreRegistrationData] @BatchID = 1
-- =============================================*/

CREATE PROCEDURE [enrollment].[usp_ValidateRegularPreRegistrationData]
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear previous error messages for this batch before re-validation
    UPDATE [enrollment].[EnrollmentPreRegistrationBatch]
    SET [ErrorMessage] = NULL
    WHERE [BatchId] = @BatchID
      AND [IsActive] = 1;

    -- Update the ErrorMessage for records that fail validation
    UPDATE [enrollment].[EnrollmentPreRegistrationBatch]
    SET
        [ErrorMessage] =
            CASE
                WHEN ISNULL([StudentFirstName], '') = '' THEN 'Student First Name is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([StudentLastName], '') = '' THEN 'Student Last Name is missing; '
                ELSE ''
            END +
            CASE
                WHEN [BirthDate] IS NULL THEN 'Birth Date is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([Gender], '') = '' THEN 'Gender is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([GuardianFirstName], '') = '' THEN 'Guardian First Name is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([GuardianLastName], '') = '' THEN 'Guardian Last Name is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([OfferSchoolDBN], '') = '' THEN 'Offer School DBN is missing; '
                ELSE ''
            END +
            CASE
                WHEN ISNULL([GradeCode], '') = '' THEN 'Grade Code is missing; '
                ELSE ''
            END
    WHERE [BatchId] = @BatchID
      AND [IsActive] = 1
      AND (
            ISNULL([StudentFirstName], '') = ''
         OR ISNULL([StudentLastName], '') = ''
         OR [BirthDate] IS NULL
         OR ISNULL([Gender], '') = ''
         OR ISNULL([GuardianFirstName], '') = ''
         OR ISNULL([GuardianLastName], '') = ''
         OR ISNULL([OfferSchoolDBN], '') = ''
         OR ISNULL([GradeCode], '') = ''
      );
END
GO
