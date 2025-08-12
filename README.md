DB2_HOST=localhost
DB2_PORT=3306
DB2_USER=stockAdm
DB2_PASSWORD=09stockAdm1@
DB2_NAME=kstock
DB2_CHARSET=utf8mb4
DB2_COLLATE=utf8mb4_unicode_ci
IP 192.168.0.198  

Fast API endPoint : http://192.168.0.198:8765/docs 


1️⃣ 실행용 배치 파일 만들기
프로젝트 루트에 run_rss.bat 생성:

bat
복사
편집
@echo off
REM 가상환경 활성화 (경로 맞게 수정)
call C:\path\to\venv\Scripts\activate

REM 파이썬 스케줄러 실행
python C:\path\to\project\run_scheduler.py
C:\path\to\... 부분을 실제 경로로 수정하세요.
가상환경 안 쓰면 python만 실행해도 됩니다.

2️⃣ PowerShell로 작업 스케줄러 등록
아래 명령 실행(경로 맞게 수정):

powershell
복사
편집
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c C:\path\to\run_rss.bat"
$Trigger = New-ScheduledTaskTrigger -Daily -At "07:00" `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Hours 17)  # 07:00~23:59
Register-ScheduledTask -TaskName "RSS_Pipeline" -Action $Action -Trigger $Trigger `
    -Description "07~23시 매시 정각 RSS 파이프라인 실행" `
    -User "$env:USERNAME" -RunLevel Highest
📌 설명

07:00 시작

1시간마다 반복

17시간 동안 유지 → 07시~23시까지

$env:USERNAME → 현재 로그인 계정으로 실행

-RunLevel Highest → 관리자 권한 실행(권장)

3️⃣ 확인 & 관리
등록 확인

powershell
복사
편집
Get-ScheduledTask -TaskName "RSS_Pipeline"
실행 로그를 남기고 싶으면 run_rss.bat 안에서:

bat
복사
편집
python C:\path\to\project\run_scheduler.py >> C:\path\to\logs\rss_%date:~-4%%date:~4,2%%date:~7,2%.log 2>&1
원하면 내가 이 PowerShell 명령 + run_rss.bat까지 통으로 프로젝트 폴더에 바로 넣을 수 있게 만들어서 줄게.
