import logging
import threading
import queue

from .connection import CreateWSClient


class robot_rt_control_async:
    def __init__(self, robot_id, wos_endpoint, queue_len: int = 1):
        self.robot_id = robot_id
        self.wos_endpoint = wos_endpoint

        self.client = CreateWSClient(self.wos_endpoint)
        if not self.client.connect():
            logging.error("Connection Failed, quitting.")
            raise RuntimeError("WOS websocket connect failed")

        self._cmd_q: queue.Queue = queue.Queue(maxsize=queue_len)

        self._running = True
        self._sender_th = threading.Thread(
            target=self._sender_loop, name="rt_sender", daemon=True
        )
        self._sender_th.start()

    def rt_move(self, target, duration):
        waypoint = {
            "position": target,
            "duration": duration,
        }
        self._enqueue({
            "method": "rt-move",
            "payload": {
                "waypoints": [waypoint],
            }
        })

    def rt_movec(self, target):
        self._enqueue({
            "method": "rt-move-cartesian",
            "payload": {
                "destination": target,
                "velocityPercentage": 100,
                "isRelative": False,
            }
        })

    def rt_movec_soft(self, target, duration):
        self._enqueue({
            "method": "rt-move-cartesian-soft",
            "payload": {
                "destination": target,
                "useVelocity": False,
                "duration": duration,
                "velocityPercentage": 0,
                "isRelative": False,
            }
        })

    def rt_movec_hard(self, target):
        self._enqueue({
            "method": "rt-move-cartesian-hard",
            "payload": {
                "destination": target,
                "isRelative": False,
            }
        })

    def _enqueue(self, cmd: dict):
        try:
            if self._cmd_q.full():
                _ = self._cmd_q.get_nowait()
            self._cmd_q.put_nowait(cmd)
        except queue.Full:
            logging.warning("cmd queue full, drop cmd")

    def _sender_loop(self):
        while self._running:
            cmd = self._cmd_q.get()
            if cmd is None:
                break
            try:
                _, err = self.client.run_request(
                    self.robot_id, cmd["method"], cmd["payload"]
                )
                if err:
                    logging.error("rt control err: %s", err)
            except Exception as e:
                logging.exception("sender_loop exception: %s", e)

    def close(self):
        self._running = False
        self._cmd_q.put(None)
        self._sender_th.join(timeout=1)
        self.client.close()
