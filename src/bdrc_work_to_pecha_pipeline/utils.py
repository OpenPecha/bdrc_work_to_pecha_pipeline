import os
import subprocess


def zip_folder(folder_path, output_path=None):
    """
    This function creates a zip file containing all files inside a folder, including files in subfolders,
    without including the top-level folder in the zip archive.
    """
    if output_path is None:
        output_path = os.path.basename(folder_path) + ".zip"

    if not os.path.isdir(folder_path):
        raise ValueError(f"The folder '{folder_path}' does not exist.")

    parent_dir = os.path.dirname(folder_path)
    folder_name = os.path.basename(folder_path)

    try:
        # Change the current working directory to the parent directory of the folder
        os.chdir(parent_dir)

        # Use subprocess to call the zip command
        # -r to include files recursively
        subprocess.run(
            ["zip", "-r", output_path]
            + [f"{folder_name}/{f}" for f in os.listdir(folder_name)],
            check=True,
        )
        print(f"Folder '{folder_name}' has been zipped successfully.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error while zipping the folder: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Change back to the original directory (good practice)
        original_cwd = os.getcwd()
        if original_cwd != parent_dir:
            os.chdir(original_cwd)

    return output_path
