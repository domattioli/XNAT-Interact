# 1. XNATLogin Validation Error

**Hypothetical Issue:** A user inputs an incorrect key in the login dictionary.

**Error and Traceback:**

```plaintext
Traceback (most recent call last):
  File "main.py", line 47, in try_login_and_connection
    validated_login = XNATLogin({'USERNAME': 'user1', 'PASSWORD': 'pass123', 'URK': 'https://example.com'})
  File "utilities.py", line 353, in __init__
    self._validate_login(input_info)
  File "utilities.py", line 372, in _validate_login
    assert all(k in validated_info for k in self.required_login_keys), f"Missing login info: {set(self.required_login_keys) - set(validated_info.keys())}"
AssertionError: Missing login info: {'URL'}
```

**Root Cause:** The user mistakenly provided the key `'URK'` instead of `'URL'`.


# 2. Template Image Matching Error

**Hypothetical Issue:** The `is_similar_to_template_image` method fails because of missing template image data.

**Error and Traceback:**

```plaintext
Traceback (most recent call last):
  File "xnat_scan_data.py", line 233, in is_similar_to_template_image
    min_val, _, _, _ = cv2.minMaxLoc(cv2.matchTemplate(self.image.processed_img, self.template_img, cv2.TM_CCOEFF_NORMED))
AttributeError: 'NoneType' object has no attribute 'processed_img'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "main.py", line 118, in upload_new_case
    valid_image = scan_file.is_similar_to_template_image()
  File "xnat_scan_data.py", line 235, in is_similar_to_template_image
    assert min_val is not None, f'BUG: template matching method should not return None type for min pixel value.'
AssertionError: BUG: template matching method should not return None type for min pixel value.
```

**Root Cause:** `template_img` is missing or improperly loaded during initialization.


# 3. ExperimentData Query Error

**Hypothetical Issue:** The `_generate_queries` method creates an invalid query string due to missing intake form attributes.

**Error and Traceback:**

```plaintext
Traceback (most recent call last):
  File "xnat_experiment_data.py", line 228, in _generate_queries
    exp_label = 'SOURCE_DATA-' + self.intake_form.uid
AttributeError: 'NoneType' object has no attribute 'uid'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "main.py", line 135, in upload_new_case
    subj_qs, exp_qs, scan_qs, files_qs, resource_label = experiment._generate_queries(xnat_connection)
  File "xnat_experiment_data.py", line 230, in _generate_queries
    raise ValueError("ExperimentData object is missing a valid IntakeForm.")
ValueError: ExperimentData object is missing a valid IntakeForm.
```

**Root Cause:** The `intake_form` object passed during instantiation was either `None` or incorrectly initialized.


# 4. ORDataIntakeForm Missing Columns Error

**Hypothetical Issue:** A user provides a data file with missing columns.

**Error and Traceback:**

```plaintext
Traceback (most recent call last):
  File "xnat_resource_data.py", line 294, in _read_from_series
    assert not missing_columns, f"Missing columns: {missing_columns}"
AssertionError: Missing columns: {'Procedure Name', 'Epic Start Time', 'Operation Date'}
 
During handling of the above exception, another exception occurred:
 
Traceback (most recent call last):
  File "main.py", line 155, in upload_new_case
    intake_form = ORDataIntakeForm(config, validated_login, input_data=pd.Series(...))
  File "xnat_resource_data.py", line 286, in __init__
    self._read_from_series(data_row, config, verbose)
ValueError: Failed to initialize ORDataIntakeForm due to missing required columns.
```

**Root Cause:** The input file is missing essential columns like `'Procedure Name'` and `'Epic Start Time'`.


# 5. ArthroDiagnosticImage Initialization Error

**Hypothetical Issue:** An invalid file format is passed during initialization.

**Error and Traceback:**

```plaintext
Traceback (most recent call last):
  File "xnat_scan_data.py", line 389, in __init__
    self._validate_input()
  File "xnat_scan_data.py", line 399, in _validate_input
    assert self.is_jpg(self.ffn), f'Inputted file must be a jpg file: {self.ffn}'
AssertionError: Inputted file must be a jpg file: /path/to/file.mp4

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "main.py", line 145, in upload_new_case
    diagnostic_image = ArthroDiagnosticImage(img_ffn=Path('/path/to/file.mp4'), still_num='1', ...)
  File "xnat_scan_data.py", line 392, in __init__
    raise ValueError(f"ArthroDiagnosticImage initialization failed due to invalid file format.")
ValueError: ArthroDiagnosticImage initialization failed due to invalid file format.
```

**Root Cause:** A `.mp4` file was mistakenly passed instead of a `.jpg`.


