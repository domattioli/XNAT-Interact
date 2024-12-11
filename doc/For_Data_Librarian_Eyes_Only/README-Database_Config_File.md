
# Database Configuration Overview

This README provides a high-level overview of the database structure and configuration, describing each table and key fields within this dataset.
*** All interactions with the database_config.json file occur in src/utilities.py (see: metatables class). ***

## Metadata

The metadata section contains general information about the database, including:
- **Creation and Modification Timestamps**: Tracks when each record was created or last modified.
- **Creator Identifiers**: Identifies the user or system responsible for creating and updating records.
- **Additional Columns**: Specific tables (e.g., `SUBJECTS`, `IMAGE_HASHES`, `SURGEONS`) contain supplementary columns, providing extra fields for each table.

## Database Tables

### 1. `REGISTERED_USERS`
- **Description**: Stores information about registered users.
- **Key Fields**:
  - `NAME`: The name of the user.
  - `UID`: A unique identifier assigned to each user.
  - `CREATED_DATE_TIME`: Timestamp for when the user was registered.
  - `CREATED_BY`: The identifier for the user or system that registered the user.

### 2. `ACQUISITION_SITES`
- **Description**: Represents various data acquisition locations, which may refer to clinical or research sites.
- **Key Fields**:
  - `NAME`: The name of the acquisition site.
  - `UID`: A unique identifier for each site.
  - `CREATED_DATE_TIME`: Timestamp for when the site was added.
  - `CREATED_BY`: Identifier of the user or system that added the site.

### 3. `GROUPS`
- **Description**: Contains identifiers for groups, which might represent clinical procedures, research groups, or cohorts.
- **Key Fields**:
  - `NAME`: The group name.
  - `UID`: A unique identifier for each group.
  - `CREATED_DATE_TIME`: Timestamp of creation.
  - `CREATED_BY`: User or system identifier for the creator.

### 4. `SUBJECTS`
- **Description**: Includes individual subjects or cases, each associated with specific sites and groups.
- **Key Fields**:
  - `NAME`: Subject or case name.
  - `UID`: Unique identifier for each subject.
  - `ACQUISITION_SITE`: The acquisition site associated with the subject.
  - `GROUP`: The group identifier linked to the subject.
  - `CREATED_DATE_TIME`: Creation timestamp.
  - `CREATED_BY`: Creator identifier.

### 5. `IMAGE_HASHES`
- **Description**: Manages image-related data, potentially to ensure image integrity (e.g., using hash values).
- **Key Fields**:
  - `HASH_VALUE`: The hash value representing the image.
  - `INSTANCE_NUM`: An instance number, potentially for organizing multiple image instances.
  - `CREATED_DATE_TIME`: Timestamp of hash creation.
  - `CREATED_BY`: Identifier for the user or system that generated the hash.

## Usage

This database structure organizes data into various tables to support structured storage and retrieval, particularly for clinical, imaging, or research contexts. Each table has designated fields to capture essential information, with metadata for tracking creation and modification.

The database_config.json file is stored on the XNAT server. The current protocol for using/updating it is:
1. Login to XNAT
2. Download database_config.json to local computer's temp folder.
3. Cross reference/update local copy as appropriate for chosen code-execution.
4. Upload revised database_config.json file back to XNAT, overwriting prior version.
5. Delete local copy.

Example location of local machine's temp directory:
`C:\Users\[username]]\AppData\Local\Temp\XNAT_Interact'
