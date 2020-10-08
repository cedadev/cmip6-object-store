import os
import subprocess


class Lotus(object):
    def __init__(self):
        pass

    def run(
        self, cmd, stdout="", stderr="", partition="short-serial", duration="00:05"
    ):

        if stdout:
            stdout = f"-o {stdout}"
        if stderr:
            stderr = f"-e {stderr}"

        batch_cmd = f"sbatch -p {partition} -t {duration} " f"{stdout} {stderr} {cmd}"

        subprocess.check_call(batch_cmd, shell=True, env=os.environ)

        print(f"Submitted: {batch_cmd}")
