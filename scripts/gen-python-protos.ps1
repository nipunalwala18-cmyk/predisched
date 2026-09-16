# PrediSched gen-python-protos
# Regenerates Python gRPC stubs from proto/ into ml/generated, mpi/generated and spark/generated.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$protoDir = Join-Path $root "proto"
foreach ($target in @("ml\generated", "mpi\generated", "spark\generated")) {
  $out = Join-Path $root $target
  New-Item -ItemType Directory -Force -Path $out | Out-Null
  New-Item -ItemType File -Force -Path (Join-Path $out "__init__.py") | Out-Null
}
python -m grpc_tools.protoc -I $protoDir --python_out="$root\ml\generated" --grpc_python_out="$root\ml\generated" (Get-ChildItem $protoDir\*.proto | ForEach-Object { $_.FullName })
python -m grpc_tools.protoc -I $protoDir --python_out="$root\mpi\generated" --grpc_python_out="$root\mpi\generated" (Get-ChildItem $protoDir\*.proto | ForEach-Object { $_.FullName })
python -m grpc_tools.protoc -I $protoDir --python_out="$root\spark\generated" --grpc_python_out="$root\spark\generated" (Get-ChildItem $protoDir\*.proto | ForEach-Object { $_.FullName })
Write-Output "Python stubs generated."
