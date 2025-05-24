import sys
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import importlib.util
import time
import json
import os
import threading
import signal
from py4j.protocol import Py4JNetworkError
class TeeLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", buffering=1)
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

class Py4jClient:
    def __init__(self, script_path, port=25333):

        print(f"[Py4jClient] Initializing Py4jClient with script_path: {script_path} and port: {port}")
        self.script_path = script_path
        self.port = port
        self.gateway = None
        self.running = False
        self.user_module = None

    def exec(self, json_msg):
        print(f"[Py4jClient] exec called with json_msg: {json_msg}")
        msg = json.loads(json_msg)
        print(f"[Py4jClient] Executing msg: {msg}")
        if msg.get("data") is None or msg.get("data") == []:
            return globals()[msg.get("method")]()
        else:
            return globals()[msg.get("method")](msg.get("data"))

    def write(self, string):
        print(f"[Py4jClient] write called with string: {string}")
        if self.py4j:
            self.py4j.handleStdOut(string)

    def flush(self):
        pass            

    def start(self):
        # Set up tee logging to both stdout and a file
        log_file = os.environ.get("PY4JCLIENT_LOG", "py4jclient.log")
        sys.stdout = TeeLogger(log_file)
        sys.stderr = TeeLogger(log_file)
        print(f"[Py4jClient] Logging to stdout and {log_file}")
        print(f"[Py4jClient] Loading user module from: {self.script_path}")
        # Load the user script into the global namespace so its functions are available in globals()
        with open(self.script_path) as f:
            code = f.read()
        exec(code, globals())
        print(f"[Py4jClient] User module loaded, starting JavaGateway...")
        try:
            print("[Py4jClient] Attempting to connect to JavaGateway...")
            self.gateway = JavaGateway(
                gateway_parameters=GatewayParameters(auto_convert=True, port=self.port, auto_close=True, read_timeout=5 ),
                callback_server_parameters=CallbackServerParameters(port=self.port + 1, daemonize_connections=True),
                python_server_entry_point=self,
            )
            print("[Py4jClient] JavaGateway JVM:", self.gateway.jvm)
            print("[Py4jClient] Successfully connected to JavaGateway!")
            print("[Py4jClient] Callback server port:", self.gateway.get_callback_server().get_listening_port())
            # No resetCallbackClient call needed for fixed port
        except Exception as e:
            print("[Py4jClient] Error connecting to JavaGateway:", e)
            self.running = False
            sys.exit(1)
        self.running = True
        self._monitor_thread = threading.Thread(target=self.monitor_connection, daemon=True)
        self._monitor_thread.start()

    def stop(self):
        print("[Py4jClient] Stopping client.")
        self.running = False
        if self.gateway is not None:
            self.gateway.shutdown()
            print("[Py4jClient] Gateway shutdown.")

    def monitor_connection(self):
        """Background thread: exits process if Java side is gone."""
        while self.running:
            try:
                self.gateway.jvm.System.currentTimeMillis()
                print("[Py4jClient] Connection alive, continuing.")
            except (Py4JNetworkError, EOFError, OSError) as e:
                print(f"[Py4jClient] Lost connection to Java: {e}, exiting.")
                self.stop()
                os._exit(1)
            time.sleep(2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Py4jClient for Lucille PythonStage integration")
    parser.add_argument('--script-path', required=True, help='Path to the user Python script to load')
    parser.add_argument('--port', required=True, type=int, help='Port for Py4J Gateway')
    args = parser.parse_args()
    script_path = args.script_path
    port = args.port
    client = Py4jClient(script_path, port)
    client.start()
    print(f"[Py4jClient] Started client with script_path: {script_path} and port: {port} leaving main")
    # client.stop()

