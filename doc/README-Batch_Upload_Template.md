
# Batch Upload Template Guide

This document is designed to facilitate batch data uploads. Each column represents specific data to be provided for each case. Please follow the instructions below for proper completion.

## Column Highlights
- **Red Columns:** These columns are required and must be filled for every entry.
- **Blue Columns:** These columns are conditionally required based on the values in the **Yellow Columns**. If a yellow column's condition is met, the corresponding blue columns must be completed.

## Key Definitions
- **Not Applicable:** Use this value when you are certain that a specific attribute or event does not exist for the case (e.g., no supervising surgeon was present).
- **Unknown:** Use this value when you cannot determine with certainty whether the attribute or event applies to the case.

## Instructions
1. Ensure all **Red Columns** are completed without exceptions.
2. Review **Blue Columns** to determine if any conditions require the completion of **Yellow Columns**.
3. Avoid leaving any yellow fields fields blank; use "Unknown" or "Not Applicable" where appropriate.
4. Refer to the 'Allowable_Inputs' tab to see permitted entries.
    - Submit a 'New Issue' on the repository if you need to request a new allowable_input (e.g., surgeon hawkid).

## Notes
- Data validation is enforced for certain columns to ensure consistency.
- Do not change anything in the Allowable_Inputs tab -- this will likely cause with errors when this document is processed by the upload code.
- Always double-check the accuracy of dates, IDs, and other sensitive information.
- Refer to the "Allowable_Inputs" sheet for acceptable values where applicable.
- Adding new values to the allowable_inputs columns also requires a manual update to the config file on the XNAT server, which can only be done by the Data Librarian.

Please contact the data team for further questions or clarifications.
