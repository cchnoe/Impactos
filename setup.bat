@echo off
cd /d "%~dp0"
python setup_env.py
:: Mueve los archivos a la carpeta env_impactos
move setup_env.py env_impactos\
move setup.bat env_impactos\
move requirements.txt env_impactos\
move create_shortcuts.ps1 env_impactos\

:: Ejecuta el script de PowerShell para crear los accesos directos
powershell -ExecutionPolicy Bypass -File create_shortcuts.ps1

pause
