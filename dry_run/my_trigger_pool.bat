@echo off
REM __version__ = "0.0.21"
REM Launch the pool-mode dry-run trigger. cmd.exe can't run a .ps1 directly,
REM so invoke PowerShell explicitly. %~dp0 = this .bat's folder (so the .ps1
REM is found regardless of the current directory).

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0my_trigger.ps1" ^
  -Mode pool -PrefectBlock yrocket -Submitter "" ^
  -PrefectApiUrl "http://192.168.0.13:4200/api" ^
  -PrefectDeployment pipeline/low_deployment ^
  -GitRepo https://github.com/ykim2718/SandBox4Git.git ^
  -GitCommit 0d19d46e2ed3b5144d2a4197b0f77ba969475678   ^
  -MinioKey electric_power_consumption/v0/powerconsumption.csv ^
  -Payload my_flow.py

REM   953096e750decf88a47520f336e4269ee1915b6e    << ~8 min
REM   0d19d46e2ed3b5144d2a4197b0f77ba969475678    << error
REM   95153312753021be0e4c09d04a387d1864de9569    << ~5 sec

