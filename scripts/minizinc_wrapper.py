import subprocess
from pathlib import Path

"""
Invokes the minizinc.exe with arguments and captures the output.
"""
class MinizincWrapper:

    minizinc_executable = Path("../../libminizinc/out/build/x64-Debug/minizinc.exe").resolve()
    def run(self, args) -> (int, list[str]):

        command = f"{MinizincWrapper.minizinc_executable} {args}"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True,
                                   cwd=MinizincWrapper.minizinc_executable.parent)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Command failed with error: {stdout.strip(), stderr.strip()}")

        return process.returncode, stdout.strip().split('\n')