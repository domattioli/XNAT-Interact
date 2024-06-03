import subprocess

def update_repo():
    try:
        # Fetch the latest changes from the remote repository
        subprocess.check_call(['git', 'fetch', 'origin'])
        
        # Merge the main branch into the current branch
        subprocess.check_call(['git', 'merge', 'origin/master'])
        
        print("Repo successfully updated.")
    except subprocess.CalledProcessError as e:
        print(f"Update failed -- error:\n{e}")

if __name__ == "__main__":
    update_repo()