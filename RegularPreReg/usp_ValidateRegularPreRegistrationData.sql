USE [OASIS]
GO

/****** Object:  StoredProcedure [enrollment].[usp_ValidatePreRegistrationData]    Script Date: 2/3/2026 8:27:25 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*-- =============================================
-- Author:		Purvesh Patel 
-- Create date: 02/03/2026
-- Description:	Validate the business rules for Regular PreReg for 2k,3k and K.
[enrollment].[usp_ValidateRegularPreRegistrationData] 
-- =============================================*/

CREATE PROCEDURE [enrollment].[usp_ValidateRegularPreRegistrationData]
AS
BEGIN
    SET NOCOUNT ON;

    -- Update the ErrorMessage for records that fail validation
    UPDATE [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
    SET 
        [ErrorMessage] = ISNULL([ErrorMessage], '') + 
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
            END
        --[TransactionStatus] = 'Failed' -- Optional: Mark the row as failed
    WHERE 
        -- Filter to only check records that haven't been validated or have errors
        (ISNULL([StudentFirstName], '') = ''
        OR ISNULL([StudentLastName], '') = ''
        OR [BirthDate] IS NULL
        OR ISNULL([Gender], '') = ''
        OR ISNULL([GuardianFirstName], '') = ''
        OR ISNULL([GuardianLastName], '') = '')
        AND [IsActive] = 1; -- Assuming you only want to validate active records
END
GO


