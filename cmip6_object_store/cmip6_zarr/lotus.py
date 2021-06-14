import os
import subprocess


class Lotus(object):
    def __init__(self):
        pass

    def run(
            self, cmd, stdout="", stderr="", partition="short-serial", duration="00:05", memory=None,
            array=None
    ):

        batch_cmd = (f"sbatch -p {partition} -t {duration}"
                     f"{f' -o {stdout}' if stdout else ''}"
                     f"{f' -e {stderr}' if stderr else ''}"
                     f"{f' --mem={memory}' if memory else ''}"
                     f"{f' --array={array}' if array else ''}"
                     f" {cmd}")

        subprocess.check_call(batch_cmd, shell=True, env=os.environ)

        print(f"Submitted: {batch_cmd}")
