from cmip6_object_store.lotus import Lotus


def test_Lotus():
    host_script = os.path.expanduser('~/get_host.sh')

    with open(host_script, 'w') as writer:
        writer.writelines(['#!/bin/bash\n', '/bin/hostname > ~/host.txt\n'])

    os.chmod(host_script, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    lotus = Lotus()
    lotus.run(host_script)