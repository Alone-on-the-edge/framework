import sys
sys.path.append("d:\Project\ssot")

import asyncio
import threading
import time
import typing

from common.logger import get_logger

def wait_until(predicate: typing.Callable[..., bool]
               ,args: typing.Optional[typing.List[typing.Any]]
               ,timeout_seconds: int = 10 * 60
               ,poll_interval_seconds: int = 30):
    max_time = time.time() + timeout_seconds
    while time.time() < max_time:
        if args is None and predicate():
            return
        elif args is not None and predicate(*args):
            return
        else:
            time.sleep(poll_interval_seconds)
    
    args_str = f"with args ({','.join(map(lambda arg: str(arg), args))})" if args is not None else 'None'
    raise TimeoutError(
        f"Timeout Error: Occured while polling functions {predicate.__name__}"
        f" with args ({args_str}) after {timeout_seconds} seconds"
    )

class BackgroundWorker:
    def __init__(self,
                 function: typing.Callable[..., None],
                 args: typing.List[typing.Any] = None,
                 sleep_interval_seconds : int = 30):
        self.function = function
        self.args = args
        self.sleep_interval_seconds = sleep_interval_seconds
        self._event_loop = asyncio.new_event_loop()
        self._is_running = True

    async def _do_run(self):
        while self._is_running:
            try:
                if self.args is None:
                    self.function()
                else:
                    self.function(*self.args)
            except Exception:
                get_logger("background-worker-logger").exception(f"Error while executing function {self.function.__name__} in background")
                raise
            await asyncio.sleep(self.sleep_interval_seconds)

    def _loop_in_thread(self):
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_until_complete(self._do_run())

    def run(self) -> None:
        t = threading.Thread(target=self._loop_in_thread, args=())
        t.setDaemon(True)
        t.start()

    def terminate(self) -> None:
        self._is_running = False