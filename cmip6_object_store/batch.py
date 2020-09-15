import os, stat
import subprocess


class Lotus(object):

    def __init__(self):
        pass

    def run(self, cmd, stdout='', stderr='', partition='short-serial',
            duration='00:05'):
        
        if stdout: stdout = f'-o {stdout}'
        if stderr: stderr = f'-e {stderr}'

        batch_cmd = f'sbatch -p {partition} -t {duration} {stdout} {stderr} {cmd}'

        subprocess.check_call(batch_cmd, shell=True)

        print(f"Submitted: {batch_cmd}")


def test_Lotus():
    host_script = os.path.expanduser('~/get_host.sh')

    with open(host_script, 'w') as writer:
        writer.writelines(['#!/bin/bash\n', '/bin/hostname > ~/host.txt\n'])

    os.chmod(host_script, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    lotus = Lotus()
    lotus.run(host_script)



class BatchManager(object):

    def __init__(self, project, rerun=False):
        self._project = project
        self._rerun = rerun

    def _get_batch(self, id):
        for batch in batch:
            if not done or self._rerun:
                run_batch(id)

    def run_batch(self, id):
        convert id to lotus command
        lotus = Lotus()
        lotus.run(cmd)

        