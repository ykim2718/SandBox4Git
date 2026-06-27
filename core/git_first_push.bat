@echo on
set USER_NAME=ykim2718
for %%I in (.) do set FOLDER=%%~nxI

echo User: %USER_NAME%
echo Repo: %FOLDER%

curl -s -o nul -w "%%{http_code}" https://github.com/%USER_NAME%/%FOLDER% | findstr "200" >nul
if errorlevel 1 (
    echo.
    echo [ERROR] GitHub repository 가 존재하지 않습니다.
    echo         https://github.com/%USER_NAME%/%FOLDER%
    echo         GitHub 에서 repository 를 먼저 생성하세요.
    echo.
    pause
    exit /b 1
)

git init
git add .
git commit -m "First commit"
git branch -M main
git remote add origin https://github.com/%USER_NAME%/%FOLDER%.git
git push -u origin main
echo Done.
pause