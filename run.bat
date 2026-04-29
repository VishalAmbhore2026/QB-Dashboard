@echo off
REM =====================================================
REM   QB Dashboard - launcher (Windows)
REM =====================================================

if not exist venv (
  echo.
  echo  Virtual environment not found. Running setup first...
  echo.
  call setup.bat
)

call venv\Scripts\activate.bat

echo.
echo =====================================================
echo   Starting QB Management Dashboard...
echo   Open your browser to:  http://localhost:5000
echo   Press Ctrl+C in this window to stop.
echo =====================================================
echo.

REM Auto-open the browser after a short delay
start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:5000"

python app.py

pause
