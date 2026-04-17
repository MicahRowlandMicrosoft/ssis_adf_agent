"""Test SSIS->ADF conversion against customer's 3-package project."""
import json
import traceback
from pathlib import Path
from ssis_adf_agent.parsers.readers.local_reader import LocalReader
from ssis_adf_agent.analyzers.complexity_scorer import score_package_detailed
from ssis_adf_agent.analyzers.gap_analyzer import analyze_gaps
from ssis_adf_agent.generators.linked_service_generator import generate_linked_services
from ssis_adf_agent.generators.dataset_generator import generate_datasets
from ssis_adf_agent.generators.dataflow_generator import generate_data_flows
from ssis_adf_agent.generators.pipeline_generator import generate_pipeline
from ssis_adf_agent.warnings_collector import WarningsCollector
from ssis_adf_agent.deployer.adf_deployer import AdfDeployer

src = Path(r'C:\Users\rowlandmicah\Downloads\Project')
out_root = Path('C:/source/ssis_adf_agent/test_output/customer')
out_root.mkdir(parents=True, exist_ok=True)

reader = LocalReader()
paths = sorted(reader.scan(str(src), recursive=False))
print(f'Found {len(paths)} packages')
print(f'Project.params present: {(src / "Project.params").exists()}')
print()

print('=' * 120)
print('PHASE 1: PARSE + ANALYZE')
print('=' * 120)
print(f'{"Package":<40} {"Score":>5} {"Tasks":>5} {"Comps":>5} {"MR":>3} {"Wn":>3} {"PkgPar":>6} {"PrjPar":>6}  Components')
print('-' * 140)

results = []
for p in paths:
    name = Path(p).stem
    try:
        with WarningsCollector() as wc:
            pkg = reader.read(Path(p))
            score, _ = score_package_detailed(pkg)
            gaps = analyze_gaps(pkg)
            mr = sum(1 for g in gaps if g.severity == 'manual_required')
            wn = sum(1 for g in gaps if g.severity == 'warning')
            comps = sum(len(t.components) for t in pkg.tasks if hasattr(t, 'components'))
            comp_types = sorted({c.component_type for t in pkg.tasks if hasattr(t, 'components') for c in t.components})
            pkg_params = len(pkg.parameters or [])
            prj_params = len(getattr(pkg, 'project_parameters', None) or [])
            results.append((name, pkg, score.score, comps, mr, wn, len(wc.warnings), comp_types, gaps))
            print(f'{name:<40} {score.score:>5} {len(pkg.tasks):>5} {comps:>5} {mr:>3} {wn:>3} {pkg_params:>6} {prj_params:>6}  {comp_types[:6]}')
    except Exception as e:
        print(f'{name:<40}  ERROR: {e}')
        traceback.print_exc()
        results.append((name, None, -1, 0, 0, 0, 0, [], []))

print()
print('=' * 120)
print('PHASE 2: CONVERT')
print('=' * 120)

convert_results = []
for name, pkg, *_ in results:
    if pkg is None:
        continue
    out_dir = out_root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        with WarningsCollector() as wc:
            ls = generate_linked_services(pkg, out_dir)
            ds = generate_datasets(pkg, out_dir)
            dfs = generate_data_flows(pkg, out_dir)
            pipeline = generate_pipeline(pkg, out_dir, stubs_dir=out_dir / 'stubs')

        deployer = AdfDeployer.__new__(AdfDeployer)
        issues = deployer.validate_artifacts(out_dir)
        convert_results.append((name, len(ls), len(ds), len(dfs), len(wc.warnings), len(issues), issues[:3]))
    except Exception as e:
        tb = traceback.format_exc()
        convert_results.append((name, 0, 0, 0, 0, -1, [f'EXC: {e}', tb[:600]]))

print(f'{"Package":<40} {"LS":>3} {"DS":>3} {"DF":>3} {"Warn":>4} {"Issues":>6}  Sample Issues')
print('-' * 120)
for name, ls, ds, dfs, warn, issues, sample in convert_results:
    print(f'{name:<40} {ls:>3} {ds:>3} {dfs:>3} {warn:>4} {issues:>6}  {sample[:2]}')

print()
print('=' * 120)
print('PHASE 3: PIPELINE STRUCTURE')
print('=' * 120)
for name, pkg, *_ in results:
    if pkg is None:
        continue
    pf_dir = out_root / name / 'pipeline'
    if not pf_dir.exists():
        continue
    for pf in sorted(pf_dir.glob('*.json')):
        print(f'\n--- {pf.relative_to(out_root)} ---')
        data = json.loads(pf.read_text())
        props = data.get('properties', {})
        params = props.get('parameters', {})
        acts = props.get('activities', [])
        print(f'  parameters ({len(params)}): {list(params.keys())}')
        print(f'  activities ({len(acts)}):')
        for a in acts[:15]:
            print(f'    - {a.get("name")} ({a.get("type")})')
