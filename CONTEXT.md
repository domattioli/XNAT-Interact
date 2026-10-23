# XNAT-Interact

Tools that move operating-room imaging cases onto a research XNAT server with patient identifiers removed.

## Language

**Operation date**:
The date of the surgery as recorded on the intake form; the single source for every date on an uploaded case.
_Avoid_: study date, acquisition date, surgery date (when meaning the DICOM tag)

**Limited Data Set**:
The IRB-approved status of the XNAT server under which real dates may be kept but direct identifiers may not.
_Avoid_: LDS (in user-facing text), de-identified data

**Twin case**:
A synthetic case built to copy the structure of a real case, with fake identifiers planted at known places.
_Avoid_: fake case, mock data

**Ground truth manifest**:
The list of every identifier planted in a **twin case**, used to score de-identification.

**Device profile**:
The stored list of screen areas where a given imaging machine burns text into its images.
_Avoid_: device template

**Pixel review**:
The check of image pixels for burned-in identifiers, ending in one verdict: clean, redact or quarantine.
_Avoid_: OCR check, PHI scan

## Example dialogue

> **Dev:** "The twin case for 015 has its PatientName planted. Does the ground truth manifest expect the operation date gone too?"
> **Domain expert:** "No. The server is a Limited Data Set, so the operation date stays. Only identifiers like names and serial numbers must be gone."
