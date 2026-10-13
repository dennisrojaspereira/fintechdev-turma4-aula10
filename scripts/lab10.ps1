# Aula 10: controlador do Game Day de Disaster Recovery (equivalente a lab10.sh).
#   scripts/lab10.ps1 up | status | chaos start | promote --autorizado-por NOME | check | report ...
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
python "$dir\aula10\lab10.py" @args
exit $LASTEXITCODE
