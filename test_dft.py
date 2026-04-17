"""Test the converter against the SSISDataFlowTraining package set."""
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

src = Path('C:/source/SSISDataFlowTraining')
out_root = Path('C:/source/ssis_adf_agent/test_output/dataflow_training')
out_root.mkdir(parents=True, exist_ok=True)

reader = LocalReader()
paths = sorted(reader.scan(str(src), recursive=False))
print(f'Found {len(paths)} packages\n')

# Phase 1: parse + analyze
print('=' * 120)
print('PHASE 1: PARSE + ANALYZE')
print('=' * 120)
print(f'{"Package":<28} {"Score":>5} {"Comps":>5} {"MR":>3} {"Wn":>3} {"Warn":>4}  Components')
print('-' * 120)

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
            results.append((name, pkg, score.score, comps, mr, wn, len(wc.warnings), comp_types, gaps))
            print(f'{name:<28} {score.score:>5} {comps:>5} {mr:>3} {wn:>3} {len(wc.warnings):>4}  {comp_types}')
    except Exception as e:
        print(f'{name:<28}  ERROR: {e}')
        results.append((name, None, -1, 0, 0, 0, 0, [], []))

# Phase 2: convert each package
print()
print('=' * 120)
print('PHASE 2: CONVERT')
print('=' * 120)

convert_results = []
for name, pkg, score, comps, mr, wn, warn, types, gaps in results:
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

        # Validate
        deployer = AdfDeployer.__new__(AdfDeployer)
        issues = deployer.validate_artifacts(out_dir)

        convert_results.append((name, len(ls), len(ds), len(dfs), len(wc.warnings), len(issues), issues[:3]))
    except Exception as e:
        tb = traceback.format_exc()
        convert_results.append((name, 0, 0, 0, 0, -1, [f'EXC: {e}', tb[:400]]))

print(f'{"Package":<28} {"LS":>3} {"DS":>3} {"DF":>3} {"Warn":>4} {"Issues":>6}  Sample Issues')
print('-' * 120)
for name, ls, ds, dfs, warn, issues, sample in convert_results:
    print(f'{name:<28} {ls:>3} {ds:>3} {dfs:>3} {warn:>4} {issues:>6}  {sample[:2]}')

# Phase 3: detailed dump for the most interesting cases
print()
print('=' * 120)
print('PHASE 3: SAMPLE DSL OUTPUT (LookupDemo, MergeJoinDemo, ConditionalSplit-like)')
print('=' * 120)
for sample_name in ['LookupDemo', 'MergeJoinDemo', 'SortDemo', 'UnionAllDemo', 'PivotDemo']:
    df_dir = out_root / sample_name / 'dataflow'
    if df_dir.exists():
        for f in df_dir.glob('*.json'):
            print(f'\n--- {f.relative_to(out_root)} ---')
            data = json.loads(f.read_text())
            print(data['properties']['typeProperties'].get('script', '(no script)'))
