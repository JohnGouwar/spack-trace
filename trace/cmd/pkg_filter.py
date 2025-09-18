from typing import Optional, List, Tuple
import importlib
import shutil
import spack.repo
import spack.config
import spack.environment
import spack.compilers.config
import spack.util.parallel
import spack.concretize
from pathlib import Path
from spack.package_base import PackageBase
from spack.cmd.develop import develop
from argparse import ArgumentParser, Namespace
from spack.bootstrap import ensure_bootstrap_configuration, ensure_clingo_importable_or_raise
import spack.llnl.util.tty as tty
from spack.spec import Spec

description = "Filter packages in the builtin directory by a language dependency"
section = "environments"
level = "long"


def has_c_dependency(pkg: PackageBase):
    return "c" in pkg.dependencies_by_name(when=False).keys()

def c_pkgs_for_slice(repo: spack.repo.Repo, slice: Optional[str], ) -> List[PackageBase]:
    '''
    Extract packages from a given repo which have a dependency on C

    :param Repo repo: The spack repo being searched
    :param Optional[str] slice: Colon separated string defining a slice.
      None yields all packages
    '''
    pkgs_gen = repo.all_package_classes()
    if slice:
        start, end = slice.split(":")
        start = int(start)
        if start < 0:
            raise Exception("Slices must start at least at 0")
        if start > 0:
            for _ in range(start):
                try:
                    next(pkgs_gen)
                except StopIteration:
                    return []
        if end == "end":
            return [pkg for pkg in pkgs_gen if has_c_dependency(pkg)]
        else:
            end = int(end)
            if start > end:
                raise Exception("Start of a slice must be lower than end")
            iters = end - start
            return [pkg for _, pkg in zip(range(iters), pkgs_gen) if has_c_dependency(pkg)]
    else:
        return [pkg for pkg in pkgs_gen if has_c_dependency(pkg)]

def _best_effort_concr_task(packed_arguments: Tuple[int, str]) -> Tuple[int, Optional[Spec]]:
    '''
    Forked concretization task that simply returns None for the spec on failure
    '''
    index, spec_str = packed_arguments
    try:
        with tty.SuppressOutput(
                error_enabled=False,
                msg_enabled=False,
                warn_enabled=False
        ):
            spec = spack.concretize.concretize_one(Spec(spec_str), tests=False)
            return index, spec
    except:
        return index, None
    
def _best_effort_concretize(
        to_concretize: List[Spec]
) -> Tuple[List[spack.concretize.SpecPair], List[Spec]]:
    '''
    This is a best-effort reimplementation of `spack.concretize.concretize_separately`
    where we also return specs that fail to concretize
    '''
    args = [
      (i, str(abstract))
      for i, abstract in enumerate(to_concretize)
    ]

    # Ensure all bootstrapping is done before forking
    try:
        importlib.import_module("clingo")
    except:
        with ensure_bootstrap_configuration():
            ensure_clingo_importable_or_raise()

    # Ensure all global updates are made 
    _ = spack.repo.PATH.provider_index
    _ = spack.compilers.config.all_compilers()
    num_procs = min(len(args), spack.config.determine_number_of_jobs(parallel=True))
    successes = []
    fails = []
    for j, (i, concrete) in enumerate(
        spack.util.parallel.imap_unordered(
            _best_effort_concr_task, args, processes=num_procs, maxtaskperchild=1
        )
    ):
        percentage = (j + 1) / len(args) * 100
        candidate = to_concretize[i]
        if concrete:
            print(f"Succesfully concretized: {candidate.name}, percent complete: {percentage}")
            successes.append((candidate, concrete))
        else:
            print(f"Failed to concretize: {candidate.name}, percent complete: {percentage}")
            fails.append(candidate)
    return successes, fails

    
def best_effort_concretize_env(env: spack.environment.Environment):
        _, fails = _best_effort_concretize([s for s in env.user_specs])
        with env.write_transaction():
            for s in fails:
                env.remove(s)
            
def setup_parser(parser: ArgumentParser):
    parser.add_argument(
        "--env-path",
        type=Path,
        required=True,
        help="Path to where env will be stored"
    )
    parser.add_argument(
        "--slice",
        type=str,
        default=None,
        help="Slice of alphabetical list of C packages (e.g. 0:end, 10:1000, 1:2).\n"
        "Note that if either index overflows, it is set to the maximum value"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite --env-path if it exists"
    )

def pkg_filter(parser, args):
    builtin = spack.repo.builtin_repo()
    c_packages = [pkg.name for pkg in c_pkgs_for_slice(builtin, args.slice)]
    if args.overwrite and args.env_path.exists():
       shutil.rmtree(str(args.env_path))
    if not (args.env_path / "spack.yaml").exists():
        env = spack.environment.create_in_dir(args.env_path, init_file=None, with_view=False)
    else:
        env = spack.environment.Environment(args.env_path)
    
    with env:
        with env.write_transaction():
            for pkg_name in c_packages:
                env.add(pkg_name)
            best_effort_concretize_env(env)
            env.manifest.yaml_content['spack']['concretizer'] = {"unify": "when_possible"}
            env.manifest.changed = True
            env.write()
    
        develop_args = Namespace(
            clone = True,
            force = True,
            spec = [s.name for s in env.user_specs],
            path = None,
            build_directory = None,
            recursive = False
        )
        develop(None, develop_args)
        
