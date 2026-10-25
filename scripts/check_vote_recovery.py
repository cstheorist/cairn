#!/usr/bin/env python3
"""Fault only the task's replica-set coordinator; always restore it."""
import json
from check_votes import ROOT,COMPOSE,run
from check_replication import node
import subprocess
import os

def phase(name):
    run('--profile','test','run','--no-deps','--rm','tests','python','/tests/vote_recovery.py',name,
        'fast' if os.environ.get('CAIRN_VOTE_COMMIT')=='2' else 'votes')

def main():
    stopped=None
    try:
        for vote in ['yes','no','open']:
            run('up','-d','--no-deps','--wait','gateway',recovery='0')
            phase('prepare_'+vote)
            stopped=node(json.loads((ROOT/'artifacts/vote-recovery.json').read_text())['coordinator'])
            subprocess.run([*COMPOSE,'kill','-s','SIGKILL',stopped],cwd=ROOT,check=True)
            run('up','-d','--no-deps','--wait','gateway')
            if vote=='open': phase('check_incomplete')
            phase('check')
            run('up','-d','--no-deps','--wait',stopped)
            stopped=None
    finally:
        if stopped: run('up','-d','--no-deps','--wait',stopped)
        run('up','-d','--no-deps','--wait','gateway')

if __name__=='__main__': main()
