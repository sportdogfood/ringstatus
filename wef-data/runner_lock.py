"""Existing process lock shared by scheduled execution and deployment."""
import contextlib,os,pathlib

class WorkerBusy(RuntimeError):pass


@contextlib.contextmanager
def worker_lock(runtime):
    path=pathlib.Path(runtime)/'python-runner.lock';path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as stream:
        if path.stat().st_size==0:stream.write(b'0');stream.flush()
        stream.seek(0)
        if os.name=='nt':
            import msvcrt
            acquire=lambda:msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            release=lambda:msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
        else:
            import fcntl
            acquire=lambda:fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            release=lambda:fcntl.flock(stream.fileno(),fcntl.LOCK_UN)
        try:acquire()
        except OSError:raise WorkerBusy('Existing Python Data worker still owns the runner lock') from None
        try:yield
        finally:stream.seek(0);release()

