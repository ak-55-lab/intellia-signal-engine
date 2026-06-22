@echo off
echo.
echo ========================================
echo  Intellia Signal Engine
echo ========================================
echo.

REM Check .env exists
if not exist ".env" (
    echo ERROR: .env file not found.
    pause
    exit /b 1
)

REM Install dependencies
echo Installing dependencies...
python -m pip install -r requirements.txt --prefer-binary
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: pip install failed. See above for details.
    pause
    exit /b 1
)

echo.
echo Starting server at http://localhost:8000
echo Press Ctrl+C to stop.
echo.

cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Server failed to start. See above for details.
    pause
)
