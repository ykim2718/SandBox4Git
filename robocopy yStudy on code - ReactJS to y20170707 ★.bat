title %~0

set target=yStudy on code - ReactJS

set log_file=%~n0.log
if exist "%log_file%" (del "%log_file%")
robocopy "d:\Wolf\Homo academicus\%target%" "\\Y20170707\d\wolf\Homo academicus\%target%" ^
         /E /XO /XJD /MIR /LOG+:"%log_file%"

timeout 10
