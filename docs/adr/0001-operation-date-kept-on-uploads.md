# Uploads keep the real operation date

The XNAT server sits inside an IRB-approved Limited Data Set, where dates are allowed, so uploaded images keep the real operation date from the intake form. Every DICOM date tag (StudyDate, SeriesDate, ContentDate, AcquisitionDate, InstanceCreationDate) and the XNAT session `DATE` field take that one value. This keeps the 2024 design, which set dates from the intake form so all images in a case agree even when the imaging machine's clock is wrong. Names, IDs, addresses, device serial numbers and birth dates are still removed. The CaseDateHash fingerprint stays in place for duplicate detection.

## Considered Options

- Shifting all dates by a secret per-case offset from the librarian's salt. Rejected because the server is covered for dates.
- Blanking all dates, or keeping only the year. Rejected because both lose timing between cases.

## Consequences

The rule FR-003 in `src/services/identity.py` ("exact surgery date ... MUST NOT be stored in readable form") no longer matches this decision and must be reworded. If the server ever leaves Limited Data Set coverage, this decision must be reversed before any further upload, because a date that has been uploaded cannot be taken back.
