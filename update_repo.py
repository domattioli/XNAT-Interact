import subprocess

def is_repo_up_to_date():
    # Fetch the latest changes from the remote repository
    subprocess.run(['git', 'fetch'], check=True)

    # Check the status of the local repository
    status_output = subprocess.run(['git', 'status', '-uno'], capture_output=True, text=True, check=True)
    
    # Check if the local branch is behind the remote branch
    if 'Your branch is behind' in status_output.stdout:
        return False
    return True

def update_repo():
    try:
        # Fetch the latest changes from the remote repository
        subprocess.run( ['git', 'fetch', 'origin'], check=True )
        
        # Check if the local branch is behind the remote branch
        status_output = subprocess.run( ['git', 'status', '-uno'], capture_output=True, text=True, check=True )
        
        if 'Your branch is behind' in status_output.stdout:
            # Merge the main branch into the current branch
            subprocess.run(['git', 'merge', 'origin/master'], check=True)
            print("Repo successfully updated.")
        else:
            print("Repo is already up to date.")
    except subprocess.CalledProcessError as e:
        print(f"Update failed -- error:\n{e}")


if __name__ == "__main__":
    update_repo()