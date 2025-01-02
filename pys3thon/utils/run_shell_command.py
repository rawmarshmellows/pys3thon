import subprocess
import sys


def run_shell_command(cmd, silent=False, same_process=False):
    try:
        if not silent:
            print(cmd)
        if same_process:
            process = subprocess.Popen(
                ["bash"], stdin=subprocess.PIPE, stdout=subprocess.PIPE
            )
            process.stdin.write(cmd.encode(sys.stdout.encoding))
            output, _ = process.communicate()
        else:
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if not silent:
                for line in iter(process.stdout.readline, b""):
                    sys.stdout.write(line.decode(sys.stdout.encoding))
    except subprocess.CalledProcessError as e:
        print(e.output.decode())
