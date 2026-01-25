import sys
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import time
import json
import os
import threading
from py4j.protocol import Py4JNetworkError
from pathlib import Path

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
    # Define allowed base directories as Path objects
    ALLOWED_BASE_DIRECTORIES = [
        Path('src/test/resources').resolve(),
        Path('src/main/resources').resolve(),
        Path('python').resolve(),
        Path('src/test/resources/ExternalPythonTest').resolve(),
    ]

    def __init__(self, script_name, script_dir, port=25333):
        print(f"[Py4jClient] Initializing Py4jClient with script_dir: {script_dir}, "
              f"script_name: {script_name}, and port: {port}")
        self.script_path = self.validate_and_construct_path(script_name, script_dir)
        self.port = port
        self.gateway = None
        self.running = False
        self.user_namespace = {}

    def validate_and_construct_path(self, script_name, script_dir):
        """
        Validate script name and directory, then construct path safely.
        Returns a Path object.
        """
        if not script_name or not isinstance(script_name, str):
            raise ValueError("Script name must be a non-empty string.")

        base_name = os.path.basename(script_name)
        if base_name != script_name:
            raise ValueError(f"Script name cannot contain path separators.")

        if not base_name.endswith('.py'):
            raise ValueError(f"Script name must end with .py.")

        if '..' in base_name:
            raise ValueError(f"Script name cannot contain '..'.")

        matched_allowed_dir = None
        for allowed_dir in self.ALLOWED_BASE_DIRECTORIES:
            try:
                user_dir_abs = os.path.abspath(script_dir)
            except Exception:
                continue

            if user_dir_abs == str(allowed_dir):
                matched_allowed_dir = allowed_dir
                break

        if matched_allowed_dir is None:
            allowed_list = [str(d) for d in self.ALLOWED_BASE_DIRECTORIES]
            raise ValueError(
                f"Script directory not in allowed list.\n"
                f"Allowed: {allowed_list}\n"
                f"Got: {script_dir}"
            )

        # Create final path using validated directory and base name
        script_path = matched_allowed_dir / base_name

        if not script_path.exists():
            raise FileNotFoundError(f"Script file not found.")

        if not script_path.is_file():
            raise ValueError(f"Script file is not a regular file.")

        try:
            script_path.relative_to(matched_allowed_dir)
        except ValueError:
            raise ValueError(f"Security: Path escapes allowed directory")

        return script_path

    def exec(self, json_msg):
        msg = json.loads(json_msg)
        method_name = msg.get("method")

        if not method_name:
          raise ValueError("Missing method in message")

        func = self.user_namespace.get(method_name)
        if func is None or not callable(func):
          raise AttributeError(f"Requested method {method_name} not found or not callable")

        data = msg.get("data")
        if data is None or data == []:
          result = func()
        else:
          result = func(data)

        if result is None:
          return None

        if isinstance(result, str):
          return result
        else:
          try:
            return json.dumps(result)
          except TypeError as e:
            print(f"[Py4jClient] Result from {method_name} is not JSON-serializable: {e}")
            raise

    def start(self):
        # Set up tee logging to both stdout and a file
        log_file = os.path.abspath("python/py4jclient.log")
        sys.stdout = TeeLogger(log_file)
        sys.stderr = TeeLogger(log_file)
        print(f"[Py4jClient] Logging to stdout and {log_file}")
        print(f"[Py4jClient] Loading user module from: {self.script_path}")

        # Load the user script into isolated namespace
        code = self.script_path.read_text(encoding='utf-8')
        exec(code, self.user_namespace)
        try:
            self.gateway = JavaGateway(
                gateway_parameters=GatewayParameters(auto_convert=True, port=self.port, auto_close=True, read_timeout=5 ),
                callback_server_parameters=CallbackServerParameters(port=self.port + 1, daemonize_connections=True),
                python_server_entry_point=self,
            )
            # note that on the Java side, the Py4JRuntime uses a StreamConsumer that looks for the string "JavaGateway STARTED" in
            # the python process output. Do not change the contents of the print statement below without also updating the StreamConsumer
            print("[Py4jClient] JavaGateway STARTED. Callback server port: ", self.gateway.get_callback_server().get_listening_port())
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
            except (Py4JNetworkError, EOFError, OSError) as e:
                print(f"[Py4jClient] Lost connection to Java: {e}, exiting.")
                self.stop()
                os._exit(1)
            time.sleep(2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Py4jClient for Lucille PythonStage integration")
    parser.add_argument('--script-name', required=True, help='Name of the Python script file')
    parser.add_argument('--script-dir', required=True, help='Directory containing the Python script')
    parser.add_argument('--port', required=True, type=int, help='Port for Py4J Gateway')
    args = parser.parse_args()
    client = Py4jClient(args.script_name, args.script_dir, args.port)
    client.start()
    print(f"[Py4jClient] Started client successfully with script_name: {args.script_name}, "
          f"script_dir: {args.script_dir}, port: {args.port}")
    # client.stop()
