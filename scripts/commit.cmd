@echo off
REM Commit helper for this repo (Windows).
REM
REM Identity is pinned HERE, not per-invocation. Earlier commits were authored
REM `opencode <opencode@local>` because each agent passed -c user.name / -c
REM user.email on the command line, and a -c override beats repo config --
REM verified, not assumed. This wrapper makes that mistake unrepresentable.
REM
REM The first 18 commits keep the `opencode` attribution deliberately. It is
REM accurate -- an agent wrote them -- and rewriting published history to put a
REM human name on it would be the less honest option.
setlocal
cd /d "%~dp0.."
git -c user.name="S Siddharth" ^
    -c user.email="64076096+Sid180603@users.noreply.github.com" ^
    commit %*
