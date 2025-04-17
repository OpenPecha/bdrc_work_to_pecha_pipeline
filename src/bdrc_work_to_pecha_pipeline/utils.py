import os
import subprocess


def zip_folder(folder_path, output_path=None):
    if output_path is None:
        output_path = (
            os.path.basename(folder_path) + ".zip"
        )  # Use the folder name for the zip

    if not os.path.isdir(folder_path):
        raise ValueError(f"The folder '{folder_path}' does not exist.")

    parent_dir = os.path.dirname(folder_path)
    folder_name = os.path.basename(folder_path)

    try:
        os.chdir(parent_dir)  # Change the current working directory
        subprocess.run(["zip", "-r", output_path, folder_name], check=True)
        print(
            f"Folder '{folder_name}' has been zipped successfully as '{output_path}', starting from its base."
        )
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while zipping the folder: {e}")
    finally:
        # Change back to the original directory (good practice)
        original_cwd = os.getcwd()
        if original_cwd != os.path.dirname(folder_path):
            os.chdir(original_cwd)

    return output_path
