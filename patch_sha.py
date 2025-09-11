import re
from pathlib import Path
from hashlib import file_digest

SHA256_RE = re.compile(r'sha256="(\w*)"')
repo_path = Path(__file__).parent / "spack_repo" / "trace_repo" / "packages"

TO_PATCH = [
    ("tracing_compiler_wrapper", "cc.sh"),
    ("mqsend", "mqsend.c")
]

for (pkg, file) in TO_PATCH:
    package_path = repo_path / pkg
    full_file_path =  package_path / file
    package_py_path = package_path / "package.py"
    with open(full_file_path, "rb") as f:
        shasum = file_digest(f, "sha256").hexdigest()
    with open(package_py_path, "r") as f:
        original_text = f.read()
    new_text = re.sub(SHA256_RE, f'"{shasum}"', original_text)
    with open(package_py_path, "w") as f:
        f.write(new_text)
        
    
