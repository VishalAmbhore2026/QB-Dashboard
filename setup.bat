@echo off
REM =====================================================
REM   QB Dashboard - first-time setup (Windows)
REM   Run this ONCE after extracting the zip.
REM =====================================================

echo.
echo [1/3] Checking for Python...
where python >nul 2>nul
if %errorlevel% neq 0 (
  echo.
  echo  ERROR: Python is not installed or not in PATH.
  echo  Please install Python 3.10 or later from:
  echo    https://www.python.org/downloads/
  echo  During install, CHECK the box "Add python.exe to PATH".
  echo.
  pause
  exit /b 1
)
python --version

echo.
echo [2/3] Creating virtual environment...
if not exist venv (
  python -m venv venv
)

echo.
echo [3/3] Installing dependencies...
call venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo =====================================================
echo   Setup complete!
echo   Double-click run.bat to start the dashboard.
echo =====================================================
echo.
pause
