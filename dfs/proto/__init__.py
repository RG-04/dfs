# gRPC codegen produces flat imports like `import master_pb2`.
# Adding this directory to sys.path makes those imports resolve
# correctly when the generated files are used as a package.
import sys
import os

_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)
