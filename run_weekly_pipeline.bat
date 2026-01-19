@echo off
setlocal enabledelayedexpansion

rem Resolve automation directory (this .bat location)
set "AUTOMATION_DIR=%~dp0"

rem Calculate last Monday date (YYYY-MM-DD) - always previous week's Monday
for /f %%i in ('powershell -NoProfile -Command "$d=Get-Date; $days=(([int]$d.DayOfWeek+6)%%7); if($days -eq 0){$days=7}; $m=$d.AddDays(-$days); $m.ToString('yyyy-MM-dd')"') do set "RUN_DATE=%%i"

rem Define dated paths
set "INPUT_FILE=%AUTOMATION_DIR%data\input\%RUN_DATE%\keywords_last_monday_%RUN_DATE%.xlsx"
set "SPLIT_DIR=%AUTOMATION_DIR%data\split\%RUN_DATE%"
set "A1_OUT=%AUTOMATION_DIR%data\assistant1_output\%RUN_DATE%"
set "A2_OUT=%AUTOMATION_DIR%data\assistant2_output\%RUN_DATE%"
set "MERGED_FILE=%AUTOMATION_DIR%data\merged\merged_last_monday_%RUN_DATE%.xlsx"

echo ============================================================================
echo RUN DATE (last Monday): %RUN_DATE%
echo ============================================================================

rem 1) Pull + filter last Monday
python "%AUTOMATION_DIR%pull_and_filter_last_monday.py"
if %errorlevel% neq 0 exit /b %errorlevel%

rem 2) Split input
python "%AUTOMATION_DIR%split_input_excel.py" --input "%INPUT_FILE%" --output-dir "%SPLIT_DIR%"
if %errorlevel% neq 0 exit /b %errorlevel%

rem 3) Assistant 1 (use dated folders)
set "RUN_DATE=%RUN_DATE%"
set "INPUT_FOLDER=%SPLIT_DIR%"
set "OUTPUT_FOLDER=%A1_OUT%"
python "%AUTOMATION_DIR%run_batch_assistant1.py"
if %errorlevel% neq 0 exit /b %errorlevel%

rem 4) Assistant 2 (use dated folders)
set "INPUT_FOLDER=%A1_OUT%"
set "OUTPUT_FOLDER=%A2_OUT%"
python "%AUTOMATION_DIR%run_batch_assistant2.py"
if %errorlevel% neq 0 exit /b %errorlevel%

rem 5) Merge Assistant 2 outputs
python "%AUTOMATION_DIR%merge_assistant2_output.py" --input-folder "%A2_OUT%" --output "%MERGED_FILE%"
if %errorlevel% neq 0 exit /b %errorlevel%

rem 6) Push merged file
python "%AUTOMATION_DIR%push_merged_items.py" --input "%MERGED_FILE%"
if %errorlevel% neq 0 exit /b %errorlevel%

echo ============================================================================
echo Pipeline completed successfully.
echo ============================================================================
