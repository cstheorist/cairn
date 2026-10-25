#!/usr/bin/env python3
"""Deterministic native vote checks, then normal recovery-enabled SQL tests."""
import os
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
COMPOSE=['docker','compose','-f','compose.replication.yaml','-f','compose.votes.yaml']

def run(*arguments,recovery='1'):
    subprocess.run([*COMPOSE,*arguments],cwd=ROOT,env=dict(os.environ,CAIRN_RECOVERY=recovery),check=True)

def main():
    try:
        run('up','-d','--build','--wait','gateway',recovery='0')
        run('up','-d','--no-deps','--force-recreate','--wait','gateway',recovery='0')
        run('--profile','test','run','--no-deps','--rm','tests','python','/tests/vote_protocol.py',recovery='0')
        run('--profile','test','run','--no-deps','--rm','tests','python','/tests/local_finish.py',recovery='0')
    finally:
        run('up','-d','--no-deps','--wait','gateway')
    run('--profile','reference','up','-d','--wait','reference')
    for suite in ['extended_cycles','transactions','mutations','relational','transaction_protocol','local_commit','transaction_start','scans','read_batches']:
        run('--profile','test','run','--no-deps','--rm','tests','python',f'/tests/{suite}.py')

if __name__=='__main__': main()
