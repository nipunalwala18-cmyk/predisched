"""The generated gRPC stubs in ``ml/generated`` (flat ``import task_pb2`` style, so the folder goes
on ``sys.path``). Regenerate with the command in RULES:

    python -m grpc_tools.protoc -I proto --python_out=ml/generated --grpc_python_out=ml/generated proto/*.proto
"""

import sys

from . import ML_DIR

GENERATED = ML_DIR / "generated"
if str(GENERATED) not in sys.path:
    sys.path.insert(0, str(GENERATED))

import prediction_pb2  # noqa: E402,F401
import prediction_pb2_grpc  # noqa: E402,F401
import task_pb2  # noqa: E402,F401
