import subprocess
from pathlib import Path

"""
Invokes the minizinc.exe with arguments and captures the output.
"""
class MinizincWrapper:

    minizinc_executable = Path(f"{Path(__file__).parent}/../libminizinc/out/build/x64-Debug/minizinc.exe").resolve()

    @staticmethod
    def run(args, stdin=None) -> (int, list[str]):

        command = f"{MinizincWrapper.minizinc_executable} {args}"
        result = subprocess.run(command,
                                shell=True,
                                text=True,
                                input=stdin,
                                cwd=MinizincWrapper.minizinc_executable.parent,
                                capture_output=True,
        )

        if result.returncode != 0:
            print(f"Command failed with error: {result.stdout.strip(), result.stderr.strip()}")

        return result.returncode, result.stdout.strip().split('\n')