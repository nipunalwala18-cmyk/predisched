"""Send one JSON PredictRequest to a prediction server and print the JSON answer, the same as

    grpcurl -plaintext -d @ localhost:50070 predisched.PredictionService/Predict < ml/testdata/predict-request.json

    python -m predisched_ml.client [--target localhost:50070] [--health] < request.json
"""

from __future__ import annotations

import argparse
import sys

import grpc
from google.protobuf import json_format

from .proto import prediction_pb2 as pb
from .proto import prediction_pb2_grpc as pb_grpc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", default="localhost:50070")
    parser.add_argument("--health", action="store_true", help="call Health instead of Predict")
    parser.add_argument("--timeout-ms", type=float, default=2000)
    args = parser.parse_args(argv)
    with grpc.insecure_channel(args.target) as channel:
        stub = pb_grpc.PredictionServiceStub(channel)
        try:
            if args.health:
                response = stub.Health(pb.HealthRequest(), timeout=args.timeout_ms / 1000)
            else:
                request = json_format.Parse(sys.stdin.read(), pb.PredictRequest())
                response = stub.Predict(request, timeout=args.timeout_ms / 1000)
        except grpc.RpcError as e:
            print(f"ERROR:\n  Code: {e.code().name}\n  Message: {e.details()}", file=sys.stderr)
            return 1
    print(json_format.MessageToJson(response))
    return 0


if __name__ == "__main__":
    sys.exit(main())
