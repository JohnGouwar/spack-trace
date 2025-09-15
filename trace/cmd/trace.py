from collections import defaultdict
import sys
import os
import json
import spack.cmd.uninstall
from spack.error import SpackError
from argparse import ArgumentParser
from spack.spec import Spec
from pathlib import Path
import spack.config
import spack.environment
import spack.concretize
import spack.repo
import spack.package_base
import spack.cmd.develop
from spack.cmd.common import arguments
from spack.cmd import parse_specs
from multiprocessing import Process
import select
from spack.llnl.util.tty import SuppressOutput, msg_enabled
from typing import List, Dict, TypedDict, Optional
try:
    from spack.extensions.trace.PosixMQ import PosixMQ
except:
    from PosixMQ import PosixMQ
COMPILE_COMMANDS_MQ="/spacktracecc"
TRACE_ROOT = Path(__file__).parent.parent.parent
TRACE_REPO = TRACE_ROOT / "spack_repo" / "trace_repo"
# The installer proc will send this when it completes to notify the listener
# that its safe to stop listening, note that the priority must be lower than any
# compiler logging message to ensure that none are dropped
DONE_MSG = "DONE"
DONE_MSG_PRIO = 0

description = "Generate a compile_commands.json for all develop specs in an environment"
section = "environments"
level = "long"

class CompileCommand(TypedDict):
    arguments: List[str]
    directory: str
    file: str
    output: Optional[str]


def _installer_proc(env: spack.environment.Environment):
    '''
    Run the installer for an environment and then notify the listener on exit,
    Note: should be run as a subprocess from `trace_compiler_calls`
    '''
    try:
        env.install_all()
    except Exception as e:
        print(e)
    finally:
        mq = PosixMQ.open(COMPILE_COMMANDS_MQ)
        mq.send(DONE_MSG, DONE_MSG_PRIO)
        mq.close()

            

def trace_compiler_calls(env: spack.environment.Environment) -> List[str]:
    '''
    This function does three things:
    1. Fork the appropriate installer
    2. Listen for traced compile commands on a POSIX message queue.
    3. When the installer completes, this then unlinks the message queue and
    returns all of the collected messages 
    '''
    mq = PosixMQ.create(COMPILE_COMMANDS_MQ)
    try:
        # Fork installation process,
        Process(
            target=_installer_proc, args=(env,)
        ).start()
        # poll process and mq
        poller = select.epoll()
        poller.register(mq.fd, select.EPOLLIN)
        messages = []
        while True:
            events = poller.poll()
            for _, ev in events:
                if ev == select.EPOLLIN:
                    msg = mq.recv()
                    if msg == DONE_MSG:
                        return messages
                    else:
                        messages.append(msg)
                else:
                    print("Unrecognized event")
                
    finally:
        mq.unlink()


        
def proc_raw_messages(
        specs_by_hash: Dict[str, Spec],
        messages: List[str]
) -> Dict[Spec, List[CompileCommand]]:
    '''
    Given the raw traced messages, separate them by their associated spec and
    generate a well formed CompileCommand
    '''
    def _extract_c_file_and_output_from_args(args):
        try:
            output = args[args.index("-o") + 1]
        except:
            output = None
        return (args[-1], output)
    
    raw_compile_commands = []
    for msg in messages:
        hash, wd, cmd, mode = msg.split(":")
        if mode == "cc":
            raw_compile_commands.append((hash, wd, cmd))
    compile_commands = defaultdict(list)
    for (hash, wd, raw_ccs) in raw_compile_commands:
        args = raw_ccs.split("\x07")
        file, output = _extract_c_file_and_output_from_args(args)
        comp_cmd : CompileCommand = {
            "arguments": args,
            "directory": wd,
            "file": file,
            "output": output,
        } 
        compile_commands[specs_by_hash[hash]].append(comp_cmd)
    return compile_commands


def concretize_tracing_wrapper(wrapper_cache_path: Optional[Path] ) -> Spec:
    '''
    Either fetch or reconcretize `tracing-compiler-wrapper` from `wrapper_cache_path`

    Setting wrapper_cache_path to None disables caching
    '''
    if wrapper_cache_path:
        full_wrapper_filename = (wrapper_cache_path / "tracing-compiler-wrapper.spec.json")
    else:
        full_wrapper_filename = None
    
    # Fast-path: fetch from a cache
    if full_wrapper_filename and full_wrapper_filename.exists():
        with open(full_wrapper_filename, "r") as f:
            return Spec.from_json(f)

    # Slow-path: concretize the tracing wrapper
    tracing_wrapper = spack.concretize.concretize_one(Spec("tracing-compiler-wrapper"))
    if full_wrapper_filename:
        with open(full_wrapper_filename, "w") as f:
            tracing_wrapper.to_json(f)
    return tracing_wrapper
    

def wrap_spec(
        spec_pair: spack.concretize.SpecPair,
        tracing_wrapper: Spec,
        env: spack.environment.Environment
) -> Spec:
    """
    Take an already concretized develop spec and replace it's 'compiler-wrapper'
    to `tracing_wrapper`. If provided an environment, this also patches the spec
    temporarily in that environment.
    """
    # Extract the full spec from the env
    user_spec, old_concr_spec = spec_pair
    wrapped_spec = old_concr_spec.copy(deps=False)
    wrapped_spec.clear_caches(ignore=("package_hash",))
    for edge in old_concr_spec.edges_to_dependencies():
        if edge.spec.name == "compiler-wrapper":
            wrapped_spec.add_dependency_edge(
                tracing_wrapper,
                depflag=edge.depflag,
                virtuals=edge.virtuals
            )
        else:
            wrapped_spec.add_dependency_edge(
                edge.spec,
                depflag=edge.depflag,
                virtuals=edge.virtuals
            )
    # Swap the patched spec into the environment
    # See spack.environment.remove for the idea 
    env_index = env.concretized_user_specs.index(user_spec)
    user_spec = env.concretized_user_specs[env_index].copy()
    del env.concretized_user_specs[env_index]
    
    dag_hash = env.concretized_order[env_index]
    del env.concretized_order[env_index]
    del env.specs_by_hash[dag_hash]
    env._add_concrete_spec(user_spec, wrapped_spec)
    return wrapped_spec

def setup_parser(parser: ArgumentParser):
    arguments.add_common_arguments(parser, ["jobs", "concurrent_packages", "specs"])
    arguments.add_concretizer_args(parser)
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=str(TRACE_ROOT / ".trace-cache"),
        help="Where to store cached_output for this extension (defaults to spack-trace/.trace-cache)\n"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Do not cache anything related to this extension"
    )

        
def trace(parser, args):
    '''
    The main command function
    '''
    env = spack.cmd.require_active_env("trace")
    with spack.config.override("repos:trace_repo", str(TRACE_REPO)):
        if args.no_cache:
            cache_dir = None
        else:
            cache_dir = Path(args.cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
        tracing_wrapper = concretize_tracing_wrapper(cache_dir)
        env = spack.cmd.require_active_env(cmd_name="trace")
        if not env.dev_specs:
            raise SpackError("spack trace requires at least one dev-spec to trace compiles")
        with env.write_transaction():
            env.concretize()
            env.write()
        wrapped_specs = [
            wrap_spec(sp, tracing_wrapper, env)
            for sp in env.concretized_specs()
            if sp[1].is_develop
        ]
        specs_by_hash = env.specs_by_hash
        raw_messages = trace_compiler_calls(env)
        spec_src_dirs : Dict[Spec, str]= {
            ws: str(ws.variants.get("dev_path").value) for ws in wrapped_specs
        }
        try:
            compile_commands_by_spec = proc_raw_messages(specs_by_hash, raw_messages)
            for spec, compile_commands in compile_commands_by_spec.items():
                src_path = spec_src_dirs.get(spec, "")
                output_json = Path(src_path) / "compile_commands.json" 
                print(f"Logged commands for {spec.name} to {output_json}")
                with open(output_json, "w") as f:
                    json.dump(compile_commands, f,indent=2)
        except Exception as e:
            print(e)
        finally:
            with SuppressOutput(
                    msg_enabled=False,
                    warn_enabled=False,
                    error_enabled=False
            ):
                for ws in wrapped_specs:
                    spack.package_base.PackageBase.uninstall_by_spec(ws, force=True)
                spack.package_base.PackageBase.uninstall_by_spec(tracing_wrapper, force=True)
    
