# Regular Pre-Registration - Execution Plan & Database Object Inventory
**Project:** OASIS FDOS'25 Pre-Registrations (2K, 3K, K)
**Last Updated:** 03/10/2026
**Author:** Purvesh Patel

---

## SSIS Package Execution Flow

### Step 1: Data Ingestion (SSIS Data Flow Task)

**Source:** Flat File Source (pipe delimited)
- `QPKF_MAINROUND_3k_*.txt` (~3,000 rows)
- `QPKF_MAINROUND_pk_*.txt` (~26,000 rows)

**Transform:** Column mapping (17 source → 30 staging columns)
- Set `BatchId` (per file/run)
- Set `IsActive = 1`
- Set `CreatedBy = 'SSIS_PreReg'`
- Set `CreatedDate = GETDATE()`
- Set `ErrorMessage = NULL`
- Convert `studentdateofbirth` YYYYMMDD → DATE
- Map source columns (see mapping table below)

**Destination:** `[OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]`

---

### Step 2: Validation (SSIS Execute SQL Task)

```sql
EXEC [OASIS_Conv].[enrollment].[usp_ValidateRegularPreRegistrationData] @BatchID = ?
```

**What it does:**
1. Clears `ErrorMessage` for this batch
2. Validates 8 required fields: `StudentFirstName`, `StudentLastName`, `BirthDate`, `Gender`, `GuardianFirstName`, `GuardianLastName`, `OfferSchoolDBN`, `GradeCode`
3. Writes error details to `ErrorMessage` column

---

### Step 3: Error Review (SSIS Execute SQL Task - Optional/QA)

```sql
SELECT COUNT(*) AS ErrorCount
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = ? AND ErrorMessage IS NOT NULL AND IsActive = 1
```

**Decision:**
- IF `ErrorCount > threshold` → Send notification / stop pipeline
- ELSE → Continue to Step 4

---

### Step 4: Process Batch (SSIS Execute SQL Task)

```sql
EXEC [OASIS_Conv].[enrollment].[usp_ProcessPreRegistrationBatch] @BatchID = ?
```

**What it does (per row, error-safe):**
1. Cursor over valid rows (`ErrorMessage IS NULL`)
2. For each row → `EXEC [bio].[sp_InsertStudentDetails]` with all 77 parameters
3. `sp_InsertStudentDetails` internally:
   - Logs to `[stg].[StudentDetails]` (audit)
   - Logs to `[dbo].[SPRunStats]` (execution stats)
   - IF `@Student_PersonID = 0` (new student):
     - INSERT → `bio.Student` (generates `StudentRecordId`)
     - INSERT → `bio.StudentAdditionalDetail`
     - INSERT → `bio.StudentEthnicity`
     - INSERT → `enrollment.Enrollment` (status = 'PDR')
     - INSERT → `bio.StudentAddress`
     - INSERT → `bio.Guardian`
     - INSERT → `bio.GuardianAddress`
     - INSERT → `bio.StudentGuardianRelationship`
   - IF `@Student_PersonID != 0` (existing):
     - UPDATE all above tables
4. On row error → writes to `ErrorMessage`, continues next row
5. Prints summary: processed count + error count

---

### Step 5: Reconciliation (SSIS Execute SQL Task)

```sql
SELECT
    COUNT(*) AS TotalLoaded,
    SUM(CASE WHEN ErrorMessage IS NULL THEN 1 ELSE 0 END) AS Successful,
    SUM(CASE WHEN ErrorMessage LIKE 'Processing Error%' THEN 1 ELSE 0 END) AS ProcessingErrors,
    SUM(CASE WHEN ErrorMessage IS NOT NULL
         AND ErrorMessage NOT LIKE 'Processing Error%' THEN 1 ELSE 0 END) AS ValidationErrors
FROM [OASIS_Conv].[enrollment].[EnrollmentPreRegistrationBatch]
WHERE BatchId = ? AND IsActive = 1
```

---

### Step 6: Archive (SSIS File System Task - Optional)

Move processed source files to `SourceFiles\Aarchive\`

---

## Source-to-Staging Column Mapping (Step 1)

| # | Source File Column | Staging Column | Notes |
|---|---|---|---|
| 1 | `match` | `StudentId` | School match identifier |
| 2 | `osepogeneratedid` | `InputStudentID` | External system ID |
| 3 | `studentlastname` | `StudentLastName` | |
| 4 | `studentfirstname` | `StudentFirstName` | |
| 5 | `studentdateofbirth` | `BirthDate` | YYYYMMDD → DATE conversion |
| 6 | `studentgender` | `Gender` | M/F (nchar 1) |
| 7 | `studenthousenumber` | `AddressStreetNumber` | |
| 8 | `studentstreetname` | `AddressStreetName` | |
| 9 | `studentapt` | `AddressApartmentNumber` | |
| 10 | `studentcity` | `City` | |
| 11 | `studentborough` | `Borough` | M/K/X/Q/R |
| 12 | `studentstate` | `State` | NY |
| 13 | `studentzip` | `ZipCode` | |
| 14 | `offerschooldbn` | `OfferSchoolDBN` | e.g. 01M034 |
| 15 | `offergradecode` | `GradeCode` | 360=3K, 350=Pre-K |
| 16 | `parentlastname` | `GuardianLastName` | |
| 17 | `parentfirstname` | `GuardianFirstName` | |
| 18 | `parenttelephone` | `GuardianPhoneNumber` | |
| — | *(SSIS-set)* | `BatchId` | Per file/run |
| — | *(SSIS-set)* | `IsActive` | 1 |
| — | *(SSIS-set)* | `CreatedBy` | 'SSIS_PreReg' |
| — | *(SSIS-set)* | `CreatedDate` | GETDATE() |
| — | *(derive)* | `GradeLevel` | Derive from GradeCode (360→3K, 350→PK) |
| — | *(empty)* | `BatchType`, `SourceFileName`, `SchoolYearId`, `GuardianMiddleInitial`, `GuardianEmail`, `ErrorMessage` | NULL or empty |

---

## Database Object Inventory

### OASIS_Conv (Staging / Workspace)

| # | Schema | Object | Type | Purpose |
|---|---|---|---|---|
| 1 | `enrollment` | `EnrollmentPreRegistrationBatch` | Table | Staging table — SSIS loads source data here (30 columns) |
| 2 | `enrollment` | `usp_ValidateRegularPreRegistrationData` | Stored Procedure | Validates 8 required fields per batch |
| 3 | `enrollment` | `usp_ProcessPreRegistrationBatch` | Stored Procedure | Cursor-processes valid rows, calls sp_InsertStudentDetails |
| 4 | `bio` | `sp_InsertStudentDetails` | Stored Procedure | Core insert/update — creates student + enrollment + guardian records |

### OASIS (Destination) — Tables Written To

| # | Schema | Object | Type | Identity Column | Purpose |
|---|---|---|---|---|---|
| 1 | `bio` | `Student` | Table | `StudentRecordId` | Core student record |
| 2 | `bio` | `StudentAdditionalDetail` | Table | `StudentAdditionalDetailRecordId` | IEP, meal code, health, special ed details |
| 3 | `bio` | `StudentEthnicity` | Table | `StudentEthnicityId` | Student ethnicity records |
| 4 | `bio` | `StudentAddress` | Table | `StudentAddressId` | Student residential address |
| 5 | `bio` | `Guardian` | Table | `GuardianId` | Guardian/parent record |
| 6 | `bio` | `GuardianAddress` | Table | `GuardianAddressId` | Guardian address |
| 7 | `bio` | `StudentGuardianRelationship` | Table | `StudentGuardianRelationshipId` | Links student to guardian |
| 8 | `enrollment` | `Enrollment` | Table | `EnrollmentId` | Enrollment record (status PDR for pre-reg) |
| 9 | `stg` | `StudentDetails` | Table | `StudentDetailsId` | Audit/staging log of all SP calls |
| 10 | `dbo` | `SPRunStats` | Table | — | SP execution audit log |

### OASIS (Destination) — Reference/Lookup Tables (Read Only)

| # | Schema | Object | Type | Looked Up By |
|---|---|---|---|---|
| 1 | `ref` | `RefGender` | Table | `GenderCode` → `GenderId` |
| 2 | `ref` | `RefBirthPlace` | Table | `BirthPlaceCode` → `BirthPlaceId` |
| 3 | `ref` | `RefProofOfBirth` | Table | `ProofOfBirthCode` → `ProofOfBirthId` |
| 4 | `ref` | `RefLanguage` | Table | `LanguageCode` → `LanguageId` |
| 5 | `ref` | `RefStudentStatus` | Table | `StudentStatusCode` → `StudentStatusId` |
| 6 | `ref` | `RefHealthInsuranceCoverage` | Table | `HealthInsuranceCoverageCode` |
| 7 | `ref` | `RefHealthCondition` | Table | `HealthConditionCode` |
| 8 | `ref` | `RefHealthAlertStatus` | Table | `HealthAlertStatusCode` |
| 9 | `ref` | `RefEthnicity` | Table | `EthnicityCode` → `EthnicityId` |
| 10 | `ref` | `RefGeographicalCode` | Table | `GeographicalCode` → `GeographicalCodeId` |
| 11 | `ref` | `RefState` | Table | `StateCode` → `StateId` |
| 12 | `ref` | `RefBorough` | Table | `BoroughCode` → `BoroughId` |
| 13 | `ref` | `RefDistrict` | Table | `DistrictCode` → `DistrictId` |
| 14 | `ref` | `RefAdmissionCode` | Table | `AdmissionCode` → `AdmissionCodeId` |
| 15 | `ref` | `RefAdmissionReason` | Table | `AdmissionReasonCode` → `AdmissionReasonId` |
| 16 | `ref` | `RefGradeLevel` | Table | `GradeLevelCode` → `GradeLevelId` |
| 17 | `ref` | `RefGradeCode` | Table | `GradeCode` → `GradeCodeId` |
| 18 | `ref` | `RefClass` | Table | `ClassCode` + SchoolDBN → `ClassId` |
| 19 | `ref` | `RefEnrollmentStatus` | Table | 'PDR' → Pre-Draft Registration status |
| 20 | `ref` | `RefAddressType` | Table | 'RESI' → Residential address type |
| 21 | `ref` | `RefMealCode` | Table | Default meal code '5' |
| 22 | `ref` | `RefRelationshipType` | Table | Guardian relationship type |
| 23 | `ref` | `RefAddressChangeCode` | Table | Address change tracking |
| 24 | `ref` | `RefNonResidentTuition` | Table | Non-resident tuition code |
| 25 | `ref` | `RefPreRegistrationAnswerToQuestion1` | Table | Pre-reg Q1 answer |
| 26 | `ref` | `RefPreRegistrationAnswerToQuestion2` | Table | Pre-reg Q2 answer |
| 27 | `schooladmin` | `School` | Table | `SchoolDBN` → `SchoolId` + `ProviderOrganizationID` |

### OASIS — Functions

| # | Schema | Object | Type | Purpose |
|---|---|---|---|---|
| 1 | `dbo` | `fn_GetEthnicitiesFromATSCode` | Table-Valued Function | Parses ATS ethnicity code → ethnicity IDs |

---

## SSIS Package Structure (Recommended)

```
RegularPreRegistration.dtsx
│
├── [Connection Managers]
│   ├── FlatFile_3K       → QPKF_MAINROUND_3k_*.txt
│   ├── FlatFile_PK       → QPKF_MAINROUND_pk_*.txt
│   └── OLEDB_OASIS_Conv  → Server: aarnestinc, DB: OASIS_Conv
│
├── [Control Flow]
│   │
│   ├── 1. ForEach Loop Container (per source file)
│   │   └── Data Flow Task: Load Source → Staging
│   │       ├── Flat File Source (pipe delimited, 17 columns)
│   │       ├── Derived Column (BatchId, IsActive, CreatedBy, CreatedDate, GradeLevel)
│   │       ├── Data Conversion (BirthDate YYYYMMDD → DT_DBDATE)
│   │       └── OLE DB Destination → [enrollment].[EnrollmentPreRegistrationBatch]
│   │
│   ├── 2. Execute SQL Task: Validate
│   │   └── EXEC [enrollment].[usp_ValidateRegularPreRegistrationData] @BatchID = ?
│   │
│   ├── 3. Execute SQL Task: Check Error Count
│   │   └── SELECT COUNT(*) WHERE ErrorMessage IS NOT NULL
│   │   └── Precedence: IF errors > 0 → Send Mail Task (optional)
│   │
│   ├── 4. Execute SQL Task: Process Batch
│   │   └── EXEC [enrollment].[usp_ProcessPreRegistrationBatch] @BatchID = ?
│   │
│   ├── 5. Execute SQL Task: Reconciliation Report
│   │   └── SELECT counts by status
│   │
│   └── 6. File System Task: Archive source files
│       └── Move to SourceFiles\Aarchive\
```

---

## Key Parameters & Defaults

| Parameter | Value | Source |
|---|---|---|
| `@Student_PersonID` | `0` | New student — SP creates PersonID |
| `@ProviderOrganizationID` | `0` | SP derives from SchoolDBN |
| `@IsPreregister` | `1` | BRD requirement — enrollment status = 'PDR' |
| `@BypassDuplicateCheck` | `0` | Enable duplicate checking |
| `@CreatedBy` | `'SSIS_PreReg'` | System user |
| `@OutsideNewYorkCity` | `0` | NYC students |
| `@WasHLISCompleted` | `0` | Default |
| `@WasStudentRecordReceived` | `0` | Default |

---

## Open Questions for Business Analyst

1. `GradeLevel` — what value maps here vs `GradeCode` (360/350)?
2. `@AdmissionDate` — is `2026-09-01` the correct FDOS date?
3. `@AdmissionCode` and `@AdmissionReason` — what values for pre-reg?
4. `@OfficialClass` — source or default?
5. `@RefPersonalInformationVerificationCode` — `'B'` per Call script, confirm?
6. `@Ethnicity` — no source data; default value?
7. `SchoolYearId` — what value for the staging table?
8. Duplicate handling — `@BypassDuplicateCheck = 0` means SP checks, but what's expected on duplicate?
9. API Integration — what are the endpoints, auth, and payload specs?
10. Are there returning students in source files, or all new 3K/Pre-K entries?
