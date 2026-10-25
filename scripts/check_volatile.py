#!/usr/bin/env python3
"""Native no-WAL contracts followed by PostgreSQL suites on volatile OPEN."""
import os
from check_votes import run

def main():
    os.environ['CAIRN_VOTE_COMMIT']='3'
    try:
        run('up','-d','--build','--wait','gateway',recovery='0')
        run('up','-d','--no-deps','--force-recreate','--wait','gateway',recovery='0')
        run('--profile','test','run','--no-deps','--rm','tests','python','/tests/volatile_open.py',recovery='0')
    finally:
        run('up','-d','--no-deps','--wait','gateway')
    run('--profile','reference','up','-d','--wait','reference')
    for suite in ['transactions','mutations','relational','transaction_protocol','local_commit',
                  'transaction_start','scans','read_batches','transaction_contention']:
        run('--profile','test','run','--no-deps','--rm','tests','python',f'/tests/{suite}.py')

if __name__=='__main__': main()
