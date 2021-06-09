import datetime
import os
import time
import socket


class FileLock(object):
    def __init__(self, fpath, debug=False):
        self._debug = debug
        self._fpath = fpath
        dr = os.path.dirname(fpath)
        if not os.path.isdir(dr):
            os.makedirs(dr)

        self.state = "UNLOCKED"


    def _debug_print(self, msg):
        if self._debug:
            print(f"{os.getpid()} {msg}")


    def acquire(self, max_time=None, poll_interval=1):
        """
        Acquire a lock.  If max_time is specified, then the lock will be considered
        stale after that number of seconds has elapsed, and other instances will be 
        able to steal the lock.

        This is implemented by making a lock file which is a symbolic link, where the 
        target (not an existing file) contains the expiry time as well as some 
        purely informational content (hostname, pid).
        """

        proc_info = f"{socket.gethostname()}_{os.getpid()}"

        while True:
            # obtain lock by creating a symlink
            # (this should be atomic, avoid races)

            if max_time == None:
                new_lock_expiry = 0
            else:
                new_lock_expiry = time.time() + max_time

            try:
                # try to create the lock by making a symlink
                new_lock_data = f"{proc_info}_{new_lock_expiry}"
                os.symlink(new_lock_data, self._fpath)
                self._debug_print(f"Got lock: {new_lock_data}")
                break

            except FileExistsError:
                # failed to get a lock - this is the exp
                pass

            try:
                existing_lock_data = os.readlink(self._fpath)
            except OSError:
                self._debug_print("could not read lock data")
                continue

            try:
                self._debug_print(f"lock exists - says {existing_lock_data}")
            except OSError:
                pass
                
            existing_lock_expiry = float(existing_lock_data.split("_")[-1])

            # get here if lock could not be obtained

            # if the steal time is reached, remove the existing lock file
            # (although actually grabbing the lock will use the above)

            if existing_lock_expiry != 0:
                try:
                    time_remaining = existing_lock_expiry - time.time()
                    self._debug_print(f"remaining lock time: {time_remaining}")
                    if time_remaining <= 0:
                        self._debug_print("trying to remove stale lock")
                        os.remove(self._fpath)
                        self._debug_print("removed stale lock")
                except FileNotFoundError:
                    pass

            time.sleep(poll_interval)

        # we got the lock and broke from the loop
        self.state = "LOCKED"



    def release(self):
        self._debug_print("unlocking")
        if os.path.islink(self._fpath):
            try:
                os.remove(self._fpath)
            except FileNotFoundError:
                pass

        self.state = "UNLOCKED"


if __name__ == '__main__':

    from multiprocessing import Process

    def mytest(i):
        #fl = FileLock("/tmp/testlock", debug=True)
        fl = FileLock("/tmp/testlock")
        fl.acquire(max_time=10)
        time.sleep(2)
        print(f"{time.asctime()}: Hello, we are {i}")
        if i == 5:
            # one of the processes exists without releasing the lock
            print("failing")
            return
        fl.release()

    procs = [Process(target=mytest, args=(i,)) for i in range(10)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

