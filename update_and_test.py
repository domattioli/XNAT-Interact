import subprocess
import sys

def run_script(script_name):
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_name}: {e}")
        return e.returncode

def main():
    # Run update_repo.py located in the src subdirectory
    update_repo_result = run_script('src/update_repo.py')
    if update_repo_result != 0:
        print("src/update_repo.py failed. Aborting.")
        sys.exit(update_repo_result)

    # Run test_virtualenv.py
    test_script_result = run_script('test_virtualenv.py')
    if test_script_result != 0:
        print("test_virtualenv.py failed.")
        sys.exit(test_script_result)

    print("\t--\tRepo has been updated and all tests are success!\n\tReady to run main.py...")

if __name__ == '__main__':
    main()