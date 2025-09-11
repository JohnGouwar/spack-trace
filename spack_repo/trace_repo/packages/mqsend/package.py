# Copyright Spack Project Developers. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

import pathlib
from spack_repo.builtin.build_systems.generic import Package

from spack.package import *


class Mqsend(Package):
    """A standalone binary that wraps `mqueue.h`'s `mq_open()` and `mq_send()`"""

    url = f"file:///{pathlib.PurePath(__file__).parent}/mqsend.c"
    maintainers("JohnGouwar")

    version(
        "1.0",
        "1c086d40965f17d9f26d8ff275ffd60a1af5ea3bc465d71ac9b52f7bb144175e",
        expand=False
    )
    depends_on("c", type="build")
    
    def install(self, spec, prefix):
        mqsend_src = pathlib.Path(self.stage.source_path) / "mqsend.c"
        mqsend_out = pathlib.Path(self.stage.source_path) / "mqsend"
        cc = which(env.get("CC", "CC_FAIL"), required=True)
        cc("-lrt", "-o", str(mqsend_out), str(mqsend_src))
        mkdir(prefix.bin)
        install(str(mqsend_out), prefix.bin.mqsend)
