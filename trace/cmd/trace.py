from collections import defaultdict
import json
import spack.cmd.uninstall
from spack.error import SpackError
from argparse import ArgumentParser
from spack.installer import PackageInstaller
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
from spack.llnl.util.tty import SuppressOutput
from typing import List, Dict, Literal, TypedDict, Optional
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


def _env_installer_proc(env: spack.environment.Environment):
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

def _single_installer_proc(package: spack.package_base.PackageBase):
    '''
    Run the installer for a single spec and then notify the listener on exit,
    Note: should be run as a subprocess from `trace_compiler_calls`
    '''
    try:
        PackageInstaller([package], keep_stage=True, restage=True).install()
    except Exception as e:
        print(e)
    finally:
        mq = PosixMQ.open(COMPILE_COMMANDS_MQ)
        mq.send(DONE_MSG, DONE_MSG_PRIO)
        mq.close()
            

def _trace_compiler_calls(
    *,
    package: Optional[spack.package_base.PackageBase]=None,
    env: Optional[spack.environment.Environment]=None
    ) -> List[str]:
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
        if env:
            Process(
                target=_env_installer_proc, args=(env,)
            ).start()
        elif package:
            Process(
                target=_single_installer_proc, args=(package,)
            ).start()
        else:
            assert False
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


        
def _compile_commands_from_raw_messages(
        specs_by_hash: Dict[str, Spec],
        messages: List[str],
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

def _proc_all_raw_messages(specs_by_hash, messages):
    output = defaultdict(list)
    for msg in messages:
        hash, wd, cmd, mode = msg.split(":")
        output[specs_by_hash[hash]].append({
            "working_dir": wd,
            "cmd": cmd.split("\x07"),
            "mode": mode
        })
    return output
    


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
    


def _wrap_spec(
        spec_pair: spack.concretize.SpecPair,
        tracing_wrapper: Spec,
        env: Optional[spack.environment.Environment]
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
        # Ensure that tracing_wrapper has the name "compiler-wrapper" but points
        # to its actual package
        p = tracing_wrapper.package
        tracing_wrapper.name = "compiler-wrapper"
        tracing_wrapper._package = p
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
    if env:
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


def _write_compile_commands(
        raw_messages : List[str],
        specs_by_hash : Dict[str, Spec],
        spec_ccjson_paths : Dict[Spec, str],
        mode : Literal["compile_commands"] | Literal["log"]
):
    '''
    Given a list of raw messages, extract the compile commands and write the
    compile_commands.json to the source directory
    '''
    if mode == "compile_commands":
        compile_commands_by_spec = _compile_commands_from_raw_messages(specs_by_hash, raw_messages)
    elif mode == "log":
        compile_commands_by_spec = _proc_all_raw_messages(specs_by_hash, raw_messages)
    else:
        raise Exception(f"Unrecognized mode: {mode}")
    for spec, compile_commands in compile_commands_by_spec.items():
        output_json = spec_ccjson_paths.get(spec, "")
        print(f"Logged commands for {spec.name} to {output_json}")
        with open(output_json, "w") as f:
            json.dump(compile_commands, f,indent=2)
            
def _get_source_path(spec: Spec, source_root) -> Path:
    return Path(source_root) / spec.format("{name}")
def _get_spec_json_path(spec: Spec, source_root: str) -> Path:
    return _get_source_path(spec, source_root) / "trace_spec.json"

def _concretize_cli_specs(specs, source_root):
    to_concretize = []
    for spec in specs:
        spec_json_path =_get_spec_json_path(spec, source_root) 
        if spec_json_path.exists():
            with open(spec_json_path, "r") as f:
            # Already concretized
                concretized_spec = Spec.from_json(f)
            to_concretize.append((spec, concretized_spec))
        else:
            to_concretize.append((spec, None))
    if len(to_concretize) == 1:
        user_spec, pot_concr = to_concretize[0]
        if pot_concr:
            concretized = [(user_spec, pot_concr)]
        else:
            concretized = [(user_spec, spack.concretize.concretize_one(user_spec))]
    else:
        concretized = spack.concretize.concretize_together_when_possible(to_concretize)
    return concretized

def trace_cli_specs(
        specs: List[Spec],
        tracing_wrapper: Spec,
        source_root: str,
        mode: Literal["compile_commands"] | Literal["log"]
):
    '''
    Trace a single spec, storing its source-code at `source_root/NAME-HASH/spack-src`
    '''
    # spec.name accesses in a .format ensures that it always returns str
    def _get_compile_commands_path(spec: Spec) -> Path:
        if mode == "compile_commands":
            return _get_source_path(spec, source_root) / "compile_commands.json"
        elif mode == "log":
            return _get_source_path(spec, source_root) / "compile_log.json"
    concretized = _concretize_cli_specs(specs, source_root)
    for (user_spec, concrete_spec) in concretized:
        compile_commands_path = _get_compile_commands_path(concrete_spec)
        if compile_commands_path.exists():
            # We've already traced this spec, safe to skip
            print(
                f"Skipping {user_spec.name}, commands already traced at: {compile_commands_path}"
            )
            continue
        Path(_get_spec_json_path(user_spec, source_root)).parent.mkdir(parents=True, exist_ok=True)
        with open(_get_spec_json_path(user_spec, source_root), "w") as f:
            concrete_spec.to_json(f)
        wrapped = _wrap_spec((user_spec, concrete_spec), tracing_wrapper, None)
        wrapped_package = wrapped.package
        source_path = _get_source_path(concrete_spec, source_root).absolute()
        wrapped_package.path = source_path
        try:
            raw_messages = _trace_compiler_calls(package=wrapped_package)
        except KeyboardInterrupt:
            exit(1)
        except:
            continue
        specs_by_hash = {wrapped.dag_hash(): wrapped}
        spec_ccjson_paths = {wrapped: str(_get_compile_commands_path(concrete_spec))}
        try:
            _write_compile_commands(raw_messages, specs_by_hash, spec_ccjson_paths, mode)
        finally:
            with SuppressOutput(
                    msg_enabled=False,
                    warn_enabled=False,
                    error_enabled=False
            ):
                spack.package_base.PackageBase.uninstall_by_spec(wrapped, force=True)
                spack.package_base.PackageBase.uninstall_by_spec(tracing_wrapper, force=True)

def trace_env_dev_specs(env: spack.environment.Environment, tracing_wrapper: Spec, mode):
    '''
    Trace the dev-specs in an environment
    '''
    with env.write_transaction():
        env.concretize()
        env.write()
    wrapped_specs = [
        _wrap_spec(sp, tracing_wrapper, env)
        for sp in env.concretized_specs()
        if sp[1].is_develop
    ]
    specs_by_hash = env.specs_by_hash
    raw_messages = _trace_compiler_calls(env=env)
    spec_ccjson_paths : Dict[Spec, str]= {
        ws: str(ws.variants.get("dev_path").value / "compile_commands.json")
        for ws in wrapped_specs
    }
    try:
        _write_compile_commands(raw_messages, specs_by_hash, spec_ccjson_paths, mode)
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
    
def setup_parser(parser: ArgumentParser):
    arguments.add_common_arguments(parser, ["jobs", "concurrent_packages", "specs"])
    arguments.add_concretizer_args(parser)
    parser.add_argument(
        "--source-root",
        type=str,
        default=str(TRACE_ROOT / "sources"),
        help="Where the source for single specs will be stored"
        " (defaults to spack-trace/sources)"
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=str(TRACE_ROOT / ".trace-cache"),
        help="Where to store cached_output for this extension"
        " (defaults to spack-trace/.trace-cache)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Do not cache anything related to this extension"
    )
    parser.add_argument(
        "--mode",
        choices=["compile_commands", "log"],
        default="compile_commands",
        help="compile_commands generates a compile_commands.json "
        "log generates a json log of all commands traced by the wrapper"
    )

def trace(parser, args):
    '''
    The main command function
    '''
    if args.no_cache:
        cache_dir = None
    else:
        cache_dir = Path(args.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
    tracing_wrapper = concretize_tracing_wrapper(cache_dir)
    if args.specs:
        specs = parse_specs(args.specs)
        trace_cli_specs(specs, tracing_wrapper, args.source_root, args.mode)
    else:
        env = spack.cmd.require_active_env(cmd_name="trace")
        if not env.dev_specs:
            raise SpackError("spack trace requires at least one dev-spec to trace compiles")
        trace_env_dev_specs(env, tracing_wrapper, args.mode)
    
