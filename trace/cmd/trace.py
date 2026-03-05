from collections import defaultdict
import json
import spack.cmd.uninstall
from argparse import ArgumentParser
from spack.spec import Spec, dt
from pathlib import Path
import spack.config
import spack.environment
import spack.concretize
import spack.repo
import spack.package_base
import spack.cmd.develop
import spack.store
from spack.cmd.common import arguments
from spack.cmd import parse_specs
from multiprocessing import Process, Pipe
from multiprocessing.connection import Connection
import select
import spack.llnl.util.tty as tty
from epic import PosixMQ, PosixShm
COMPILE_COMMANDS_MQ="/spacktracemq"
TRACE_ROOT = Path(__file__).parent.parent.parent
# The installer proc will send this when it completes to notify the listener
# that its safe to stop listening, note that the priority must be lower than any
# compiler logging message to ensure that none are dropped
DONE_MSG = "DONE"
DONE_MSG_PRIO = 0

description = "Trace all calls to a compiler"
section = "environments"
level = "long"

def output_writer(messages_pipe: Connection, hash_to_output: dict[str, Path]):
    hash_to_commands = defaultdict(list)
    try:
        while True:
            mq_msg = messages_pipe.recv()
            shm_name, shm_size = mq_msg.split(";")
            shm = PosixShm.open(shm_name, int(shm_size))
            try: 
                data = shm.read().tobytes().decode()
                hash, wd, *cmd = data.split(";")
                hash_to_commands[hash].append({"working_dir": wd, "command": cmd})
            except Exception as e:
                raise e
            finally:
                shm.close()
                shm.unlink()
                
    finally:
        for hash, commands in hash_to_commands.items():
            with open(hash_to_output[hash], "w") as f:
                json.dump(commands, f, indent=2)
    
    
        
def mq_listener(messages_pipe: Connection):
    '''
    This function does three things:
    1. Fork the appropriate installer
    2. Listen for traced compile commands on a POSIX message queue.
    3. When the installer completes, this then unlinks the message queue and
    returns all of the collected messages 
    '''
    mq = PosixMQ.create(COMPILE_COMMANDS_MQ)
    try:
        poller = select.epoll()
        poller.register(mq.fd, select.EPOLLIN)
        while True:
            events = poller.poll()
            for _, ev in events:
                if ev == select.EPOLLIN:
                    msg = mq.recv()
                    if msg == DONE_MSG:
                        return
                    else:
                        messages_pipe.send(msg)
                else:
                    print("Unrecognized event")
                
    finally:
        messages_pipe.close()
        mq.unlink()


def setup_parser(parser: ArgumentParser):
    arguments.add_common_arguments(parser, ["jobs", "concurrent_packages", "specs"])
    arguments.add_concretizer_args(parser)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=str(TRACE_ROOT / "sources"),
        help="Where the source for single specs will be stored"
        " (defaults to spack-trace/sources)"
    )
    parser.add_argument(
        "--spec-file",
        type=Path,
        help="Newline separated file of abstract specs"
    )

def ensure_tracecc(installer):
    tracecc_spec = spack.store.STORE.db.query_one("tracecc-gcc ^gcc", installed=True)
    if tracecc_spec is not None:
        tty.info(f"Already installed {tracecc_spec.format('{name}/{hash:7}')}")
        return
    else:
        tty.info("Installing tracecc-gcc")
        tracecc_spec = spack.concretize.concretize_one("tracecc-gcc ^gcc")
        installer([tracecc_spec.package]).install()

        
def concretize_with_tracecc(specs: list[Spec], installer):
    ensure_tracecc(installer)
    for s in specs:
        s.add_dependency_edge(Spec("tracecc-gcc"), depflag=dt.BUILD, virtuals=("c",), when=Spec("%c"))
        s.add_dependency_edge(Spec("tracecc-gcc"), depflag=dt.BUILD, virtuals=("cxx",), when=Spec("%cxx"))
    # TODO: Cache concretizations in source directories
    to_concretize = [(s, None) for s in specs]
    return spack.concretize.concretize_separately(to_concretize)


def trace(parser, args): 
    '''
    The main command function
    '''
    if spack.config.get("config:installer", "old") == "new":
        from spack.new_installer import PackageInstaller
    else:
        from spack.installer import PackageInstaller
    if args.specs:
        specs = parse_specs(args.specs)
    elif args.spec_file is not None and args.spec_file.exists():
        with open(args.spec_file, "r") as f:
            specs = [Spec(l) for l in f]
    else:
        raise Exception("Must provide cli specs or existing spec file")
    packages = []
    hash_to_output_file = {}
    for _, concr in concretize_with_tracecc(specs, PackageInstaller):
        package = concr.package
        package.path = (args.source_root / concr.format("{name}")).absolute()
        packages.append(package)
        hash_to_output_file[concr.dag_hash()] = package.path / "compile_log.json"
    msg_recv, msg_send = Pipe(duplex=False)
    Process(
       target=mq_listener,
       args=(msg_send,)
    ).start()
    Process(
       target=output_writer,
       args=(msg_recv, hash_to_output_file)
    ).start()
    try:
        PackageInstaller(packages, keep_stage=True, restage=False).install()
    except Exception as e:
        print(e)
    finally:
        mq = PosixMQ.open(COMPILE_COMMANDS_MQ)
        mq.send(DONE_MSG, DONE_MSG_PRIO)
        mq.close()
    

    
