### Start script for Windows

REM Activate Python venv
call venv\Scripts\activate.bat

REM Run Watchdog2MQTT
python src\watchdog2mqtt.py

REM _optional_ keep the window open (no idea if necassary)
pause
