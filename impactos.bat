@echo off
cd /d "%~dp0"
call env_impactos\Scripts\activate
python main.py
pause
