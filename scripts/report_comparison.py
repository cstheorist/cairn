#!/usr/bin/env python3
"""Summarize complete, repeated results only when environment evidence matches."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import tarfile

DATABASES=('cairn','yugabyte','tidb')
WORKLOADS=('read','mixed','transfer')

def summarize(directory):
    evidence=json.loads((directory/'comparison-environment.json').read_text())
    summaries={}
    settings=None
    load_image=None
    for database in DATABASES:
        environment=evidence['databases'][database]
        if environment.get('status')!='accepted':
            raise ValueError(f'{database}: environment checks did not accept this measurement')
        path=directory/f'{database}-benchmark.json'
        if hashlib.sha256(path.read_bytes()).hexdigest()!=environment['result_sha256']:
            raise ValueError(f'{database}: result does not match its environment evidence')
        if 'source_archive' in environment:
            archive=directory/environment['source_archive']
            if hashlib.sha256(archive.read_bytes()).hexdigest()!=environment['source_archive_sha256']:
                raise ValueError(f'{database}: source archive hash differs')
            with tarfile.open(archive) as source:
                hashes={member.name:hashlib.sha256(source.extractfile(member).read()).hexdigest()
                        for member in source.getmembers() if member.isfile()}
            if hashes!=environment['source_sha256']:
                raise ValueError(f'{database}: source archive does not match source hashes')
        report=json.loads(path.read_text())
        if report['status']!='complete' or report['database']!=database:
            raise ValueError(f'{database}: incomplete or mislabeled results')
        if 'transaction_isolation' in report and report['transaction_isolation']!=environment['transaction_isolation']:
            raise ValueError(f'{database}: driver isolation differs from environment evidence')
        if report['seconds_per_run']<10 or report['warmup_seconds']<2:
            raise ValueError(f'{database}: diagnostic durations are too short for this report')
        current=(report['rows'],report['payload_bytes'],report['seconds_per_run'],report['warmup_seconds'],report['protocol'])
        if settings is not None and settings!=current:
            raise ValueError('workload settings differ between databases')
        settings=current
        if load_image is not None and load_image!=environment['load_image']:
            raise ValueError('load-generator images differ')
        load_image=environment['load_image']
        containers={item['service']:item for item in environment['containers']}
        if abs(sum(item['cpu_limit'] for item in containers.values())-6)>0.001 or sum(item['memory_limit_bytes'] for item in containers.values())!=6*1024**3:
            raise ValueError(f'{database}: resource budget differs')
        for after in environment['containers_after']:
            before=containers[after['service']]
            if after['state']!='running' or after['oom_killed'] or before['restarts']!=after['restarts'] or before['started_at']!=after['started_at']:
                raise ValueError(f'{database}: a database process restarted or stopped')
        if not report['transfer_partitions']['all_transfers_cross_partitions']:
            raise ValueError(f'{database}: transfer partition coverage is missing')
        summaries[database]={}
        for workload in WORKLOADS:
            runs=[row for row in report['results'] if row['workload']==workload]
            if len(runs)<3 or len({row['repeat'] for row in runs})!=len(runs):
                raise ValueError(f'{database}/{workload}: need at least three distinct repeats')
            if any(not row['correctness']['all_key_values_verified'] or not row['correctness']['verified'] for row in runs):
                raise ValueError(f'{database}/{workload}: per-key correctness check failed')
            clients={row['clients'] for row in runs}
            if len(clients)!=1:
                raise ValueError(f'{database}/{workload}: client count changed')
            rates=[row['operations_per_second'] for row in runs]
            summaries[database][workload]={
                'clients':next(iter(clients)), 'repeats':len(runs),
                'median_ops_s':statistics.median(rates),
                'mean_ops_s':statistics.mean(rates), 'sample_stddev_ops_s':statistics.stdev(rates),
                'min_ops_s':min(rates), 'max_ops_s':max(rates),
                'median_p99_us':statistics.median(row['latency_us']['p99'] for row in runs),
                'serialization_retries':sum(row['serialization_retries'] for row in runs),
            }
    for workload in WORKLOADS:
        if len({summaries[database][workload]['clients'] for database in DATABASES})!=1:
            raise ValueError(f'{workload}: concurrency differs between databases')
    ratios={workload:{database:summaries['cairn'][workload]['median_ops_s']/summaries[database][workload]['median_ops_s']
                      for database in DATABASES[1:]} for workload in WORKLOADS}
    return {'scope':'single-host, warm-cache, closed-loop development comparison; no production-wide claim',
            'settings':dict(zip(('rows','payload_bytes','seconds_per_run','warmup_seconds','protocol'),settings)),
            'isolation_levels':{database:evidence['databases'][database]['transaction_isolation'] for database in DATABASES},
            'databases':summaries,'cairn_throughput_ratio':ratios}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,default=Path(__file__).resolve().parents[1]/'artifacts')
    arguments=parser.parse_args()
    report=summarize(arguments.directory)
    (arguments.directory/'comparison-summary.json').write_text(json.dumps(report,indent=2)+'\n')
    lines=['| Workload | Cairn ops/s | YugabyteDB ops/s | TiDB ops/s | Cairn/YugabyteDB | Cairn/TiDB |',
           '| --- | ---: | ---: | ---: | ---: | ---: |']
    for workload in WORKLOADS:
        rates=[report['databases'][database][workload]['median_ops_s'] for database in DATABASES]
        ratios=report['cairn_throughput_ratio'][workload]
        lines.append(f'| {workload} | {rates[0]:,.0f} | {rates[1]:,.0f} | {rates[2]:,.0f} | {ratios["yugabyte"]:.2f}x | {ratios["tidb"]:.2f}x |')
    isolation='; '.join(f'{database}: {level}' for database,level in report['isolation_levels'].items())
    text='\n'.join(lines)+'\n\nMedians of repeated runs. '+report['scope']+'.\n\nIsolation: '+isolation+'.\n'
    (arguments.directory/'comparison-summary.md').write_text(text)
    print(text)

if __name__=='__main__': main()
