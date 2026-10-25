#!/usr/bin/env python3
"""Crash task-owned leaders at temporary/prepared/committed boundaries."""
import json
import os
import subprocess
from check_votes import ROOT,COMPOSE,run
from check_replication import node

def phase(mode,case):
    run('--profile','test','run','--no-deps','--rm','tests','python','/tests/volatile_recovery.py',mode,case)

def main():
    os.environ['CAIRN_VOTE_COMMIT']='3'
    stopped=None
    try:
        for case in ['home','remote','orphan','partial','partial_remote','wait','local','no']:
            run('up','-d','--no-deps','--wait','gateway',recovery='0')
            phase('seed',case)
            if case not in ('wait','no'):
                stopped=node(json.loads((ROOT/'artifacts/volatile-recovery.json').read_text())['dead'])
                subprocess.run([*COMPOSE,'kill','-s','SIGKILL',stopped],cwd=ROOT,check=True)
            run('up','-d','--no-deps','--wait','gateway')
            phase('check',case)
            if stopped:
                run('up','-d','--no-deps','--wait',stopped)
                stopped=None
    finally:
        if stopped: run('up','-d','--no-deps','--wait',stopped)
        run('up','-d','--no-deps','--wait','gateway')

if __name__=='__main__': main()
