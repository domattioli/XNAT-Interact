"""
packaging — detect-or-bundle launcher, update checker, and version.

Modules
-------
python_detect   : PythonDetector — injectable interpreter discovery.
launcher        : choose_runtime / prepare_and_launch — pure runtime selection
                  and launch orchestration via injected callables.
update_checker  : check_for_update — injectable, fail-soft version comparison.
"""
