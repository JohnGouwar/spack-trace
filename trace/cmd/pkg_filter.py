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
from argparse import ArgumentParser 
from spack.bootstrap import ensure_bootstrap_configuration, ensure_clingo_importable_or_raise
import spack.llnl.util.tty as tty
import spack.repo
from spack.spec import Spec

import sys
sys.path.append(spack.repo.builtin_repo().build_systems_path)
from spack_repo.builtin.build_systems.python import PythonPackage
description = "Filter packages in the builtin directory by a language dependency"
section = "environments"
level = "long"


def _is_valid_pkg(
        pkg: PackageBase,
        *,
        valid_dep_names: List[str],
        remove_python: bool = True
) -> bool:
    dep_names = pkg.dependencies_by_name(when=False).keys()
    is_python_pkg = lambda pkg: isinstance(pkg, PythonPackage)
    has_valid_deps = any(n in dep_names for n in valid_dep_names)
    if remove_python:
        return has_valid_deps and not is_python_pkg(pkg)
    else:
        return has_valid_deps
    

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
        to_concretize: List[Spec],
        success_file: Path = Path("successes.txt"),
        fail_file: Path = Path("fails.txt"),
):
    '''
    This is a best-effort reimplementation of `spack.concretize.concretize_separately`
    where we also return specs that fail to concretize
    '''
    if success_file.exists():
        with open(success_file, "r") as f:
            successes = f.readlines()
        successes = [s.strip() for s in successes if not s.isspace()]
    else:
        successes = []
    if fail_file.exists():
        with open(fail_file, "r") as f:
            fails = f.readlines()
        fails = [s.strip() for s in fails if not s.isspace()]
    else:
        fails = []
    args = [
      (i, str(abstract))
      for i, abstract in enumerate(to_concretize)
      if abstract.name not in successes and abstract.name not in fails
    ]
    if len(args) == 0:
        return
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
    
    try:
        for j, (i, concrete) in enumerate(
                spack.util.parallel.imap_unordered(
                    _best_effort_concr_task, args, processes=num_procs, maxtaskperchild=1
                )
        ):
            percentage = (j + 1) / len(args) * 100
            candidate = to_concretize[i]
            if concrete:
                print(f"Succesfully concretized: {candidate.name}, percent complete: {percentage}")
                successes.append(candidate.name)
            else:
                print(f"Failed to concretize: {candidate.name}, percent complete: {percentage}")
                fails.append(candidate.name)
    finally:
        # Flush to intermediate files
        print(f"Writing {len(successes)} successful packages to {success_file.absolute()}")
        with open(success_file, "w") as f:
            f.write("\n".join(successes))
        print(f"Writing {len(fails)} failed packages to {success_file.absolute()}")
        with open(fail_file, "w") as f:
            f.write("\n".join(fails))

    
def setup_parser(parser: ArgumentParser):
    parser.add_argument(
        "--success-file",
        type=Path,
        default=Path("successes.txt"),
        help="Text file where the names of successful packages will be stored"
    )
    parser.add_argument(
        "--fail-file",
        type=Path,
        default=Path("fails.txt"),
        help="Text file where the names of failed packages will be stored"
    )
    parser.add_argument(
        "--languages",
        type=str,
        help="A comma separated list of languages to include",
        default="c,cxx"
    )
    parser.add_argument(
        "--remove-python",
        action="store_true",
        help="Remove PythonPackages from results"
    )

def pkg_filter(parser, args):
    builtin = spack.repo.builtin_repo()
    potential_specs = [
        Spec(pkg.name)
        for pkg in builtin.all_package_classes()
        if _is_valid_pkg(
                pkg,
                valid_dep_names=args.languages.split(","),
                remove_python=args.remove_python
        )
    ]
    print(
        f"Discovered {len(potential_specs)} potential packages. "
        "Now testing for concretization..."
    )
    _best_effort_concretize(potential_specs, args.success_file, args.fail_file)
    
