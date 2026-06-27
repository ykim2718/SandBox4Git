@echo off
setlocal enabledelayedexpansion

REM === 1. Build date string: YYYY-MM-DD ===
set YYYY=%date:~0,4%
set MM=%date:~5,2%
set DD=%date:~8,2%
set TODAY=%YYYY%-%MM%-%DD%

REM === 2. Build time string: HH-MM-SS (handle leading space) ===
set HOUR=%time:~0,2%
if "%HOUR:~0,1%"==" " set HOUR=0%HOUR:~1,1%
set MIN=%time:~3,2%
set SEC=%time:~6,2%
set NOWTIME=%HOUR%-%MIN%-%SEC%

REM === 3. Combine ===
set NOW=%TODAY%_%NOWTIME%

echo Commit message: Auto commit at %NOW%

REM === 2. Initialize Git repo if .git does not exist ===
if not exist .git (
    echo Initializing new git repository...
    git init
)

REM === 3. Detect current branch ===
for /f "delims=" %%b in ('git branch --show-current') do set BRANCH=%%b

if "%BRANCH%"=="" (
    echo No branch detected. Creating main branch...
    git checkout -b main
    set BRANCH=main
)

echo Current branch: %BRANCH%

REM === 4. Stage all changes ===
git add .

REM === 5. Check if there are staged changes ===
git diff --cached --quiet
if %errorlevel%==0 (
    echo No changes to commit.
) else (
    git commit -m "Auto commit at %NOW%"
)

REM === 6. Add remote origin only if it does not exist ===
git remote | find "origin" >nul
if %errorlevel% neq 0 (
    echo Adding remote origin...
    git remote add origin https://github.com/ykim2718/Shell-Scripts.git
)

REM === 7. Push to remote ===
echo Pushing to origin %BRANCH%...
git push -u origin %BRANCH%

echo Done.
pause
