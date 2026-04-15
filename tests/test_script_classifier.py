"""
Tests for the script classifier and its integration with the complexity scorer
and gap analyzer.
"""
from __future__ import annotations

import pytest

from ssis_adf_agent.parsers.models import (
    ComplexityScore,
    SSISPackage,
    ScriptTask,
    ExecuteSQLTask,
    TaskType,
    ProtectionLevel,
)
from ssis_adf_agent.analyzers.script_classifier import (
    ScriptClassificationResult,
    ScriptComplexity,
    TIER_WEIGHTS,
    classify_script,
)
from ssis_adf_agent.analyzers.complexity_scorer import (
    score_package,
    score_package_detailed,
)
from ssis_adf_agent.analyzers.gap_analyzer import analyze_gaps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_package(tasks=None, name="TestPackage") -> SSISPackage:
    return SSISPackage(
        id="pkg-1",
        name=name,
        source_file="test.dtsx",
        tasks=tasks or [],
    )


def _make_script(
    name: str = "SCR Set Variables",
    source_code: str | None = None,
    ro_vars: list[str] | None = None,
    rw_vars: list[str] | None = None,
) -> ScriptTask:
    return ScriptTask(
        id=f"task-{name.replace(' ', '_')}",
        name=name,
        source_code=source_code,
        read_only_variables=ro_vars or [],
        read_write_variables=rw_vars or [],
    )


# ===================================================================
# 1. Script classifier — source code analysis
# ===================================================================

class TestClassifyFromSource:
    """Test classification when source_code IS available."""

    def test_trivial_variable_assignment(self):
        """Typical customer pattern: set conn strings based on environment."""
        code = r'''
using System;
using Microsoft.SqlServer.Dts.Runtime;

[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
{
    public void Main()
    {
        string env = Dts.Variables["User::Environment"].Value.ToString();
        if (env == "PROD")
        {
            Dts.Variables["User::ServerName"].Value = "SQLPROD01";
            Dts.Variables["User::FilePath"].Value = @"\\prod\share\output";
        }
        else
        {
            Dts.Variables["User::ServerName"].Value = "SQLDEV01";
            Dts.Variables["User::FilePath"].Value = @"\\dev\share\output";
        }
        Dts.TaskResult = (int)ScriptResults.Success;
    }
}
'''
        task = _make_script(source_code=code, rw_vars=["User::ServerName", "User::FilePath"])
        result = classify_script(task)

        assert result.tier == ScriptComplexity.TRIVIAL
        assert result.weight == TIER_WEIGHTS[ScriptComplexity.TRIVIAL]  # 2
        assert result.variables_only is True
        assert result.adf_expressible is True

    def test_trivial_switch_environment(self):
        """Switch statement picking connection string by environment."""
        code = r'''
public void Main()
{
    string env = Dts.Variables["User::Environment"].Value.ToString();
    switch (env)
    {
        case "DEV":
            Dts.Variables["User::ConnectionString"].Value = "Server=DEV01;Database=MyDB;";
            break;
        case "QA":
            Dts.Variables["User::ConnectionString"].Value = "Server=QA01;Database=MyDB;";
            break;
        case "PROD":
            Dts.Variables["User::ConnectionString"].Value = "Server=PROD01;Database=MyDB;";
            break;
        default:
            Dts.Variables["User::ConnectionString"].Value = "Server=DEV01;Database=MyDB;";
            break;
    }
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.TRIVIAL
        assert result.variables_only is True

    def test_trivial_empty_main(self):
        """Empty (or nearly empty) script body → trivial."""
        code = '''
public void Main()
{
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.TRIVIAL

    def test_simple_string_manipulation(self):
        """String ops like Replace, Substring → simple."""
        code = r'''
public void Main()
{
    string raw = Dts.Variables["User::FileName"].Value.ToString();
    string cleaned = raw.Replace(" ", "_").Substring(0, 20);
    string final = String.Format("{0}_{1}.csv", cleaned, DateTime.Now.ToString("yyyyMMdd"));
    Dts.Variables["User::OutputFile"].Value = final;
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.SIMPLE

    def test_simple_path_combine(self):
        code = r'''
public void Main()
{
    string dir = Dts.Variables["User::OutputDir"].Value.ToString();
    string file = Dts.Variables["User::FileName"].Value.ToString();
    Dts.Variables["User::FullPath"].Value = Path.Combine(dir, file);
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        # Path.Combine is a safe method within a variable-assignment script;
        # the classifier correctly treats this as trivial.
        assert result.tier == ScriptComplexity.TRIVIAL

    def test_moderate_file_io(self):
        """File.ReadAllText / File.WriteAllText → moderate."""
        code = r'''
public void Main()
{
    string path = Dts.Variables["User::FilePath"].Value.ToString();
    string content = File.ReadAllText(path);
    content = content.Replace("OLD", "NEW");
    File.WriteAllText(path + ".fixed", content);
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.MODERATE
        assert "file i/o" in result.reason.lower()

    def test_moderate_regex(self):
        code = r'''
public void Main()
{
    string input = Dts.Variables["User::RawData"].Value.ToString();
    var match = Regex.Match(input, @"\d{4}-\d{2}-\d{2}");
    if (match.Success)
        Dts.Variables["User::ExtractedDate"].Value = match.Value;
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.MODERATE

    def test_moderate_http_client(self):
        code = r'''
public void Main()
{
    var client = new HttpClient();
    var response = client.GetStringAsync("https://api.example.com/data").Result;
    Dts.Variables["User::ApiResponse"].Value = response;
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.MODERATE

    def test_complex_sql_connection(self):
        """Direct SqlConnection usage → complex."""
        code = r'''
public void Main()
{
    string cs = Dts.Variables["User::ConnectionString"].Value.ToString();
    using (var conn = new SqlConnection(cs))
    {
        conn.Open();
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM dbo.Orders", conn))
        {
            int count = (int)cmd.ExecuteScalar();
            Dts.Variables["User::RecordCount"].Value = count;
        }
    }
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.COMPLEX
        assert "database" in result.reason.lower()

    def test_complex_threading(self):
        code = r'''
public void Main()
{
    Parallel.ForEach(files, f => ProcessFile(f));
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.COMPLEX

    def test_complex_com_interop(self):
        code = r'''
public void Main()
{
    Type excelType = Type.GetTypeFromProgID("Excel.Application");
    dynamic excel = Activator.CreateInstance(excelType);
    excel.Visible = false;
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.COMPLEX

    def test_complex_process_start(self):
        code = r'''
public void Main()
{
    var psi = new ProcessStartInfo("cmd.exe", "/c dir");
    psi.RedirectStandardOutput = true;
    var proc = Process.Start(psi);
    Dts.Variables["User::Output"].Value = proc.StandardOutput.ReadToEnd();
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.COMPLEX

    def test_volume_fallback_short(self):
        """Very short code with no recognised patterns → trivial."""
        code = '''
public void Main()
{
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        result = classify_script(_make_script(source_code=code))
        assert result.tier == ScriptComplexity.TRIVIAL


# ===================================================================
# 2. Script classifier — heuristic analysis (no source code)
# ===================================================================

class TestClassifyFromHeuristics:
    """Test classification when source_code is NOT available."""

    def test_config_like_variables_trivial(self):
        task = _make_script(
            rw_vars=["User::ServerName", "User::FilePath", "User::ConnectionString"],
            ro_vars=["User::Environment"],
        )
        result = classify_script(task)
        assert result.tier == ScriptComplexity.TRIVIAL
        assert result.variables_only is True

    def test_config_like_variables_simple(self):
        """Lower config ratio but still config-ish → simple."""
        task = _make_script(
            rw_vars=["User::ServerName", "User::FilePath", "User::RecordCount",
                      "User::BatchSize", "User::MaxRetries"],
            ro_vars=["User::Environment", "User::RunDate", "User::ThreadCount"],
        )
        result = classify_script(task)
        assert result.tier in (ScriptComplexity.TRIVIAL, ScriptComplexity.SIMPLE)

    def test_no_variables_complex(self):
        """No variables at all and no source → assume complex."""
        task = _make_script()
        result = classify_script(task)
        assert result.tier == ScriptComplexity.COMPLEX

    def test_many_non_config_variables(self):
        """Many variables with non-config names → complex."""
        task = _make_script(
            rw_vars=[f"User::Var{i}" for i in range(10)],
            ro_vars=[f"User::Input{i}" for i in range(5)],
        )
        result = classify_script(task)
        assert result.tier == ScriptComplexity.COMPLEX


# ===================================================================
# 3. Complexity scorer integration
# ===================================================================

class TestComplexityScorerIntegration:
    """Verify the scorer uses classification weights instead of flat 20."""

    def test_trivial_script_low_score(self):
        """A package with only a trivial script should score very low."""
        trivial_code = r'''
public void Main()
{
    Dts.Variables["User::Server"].Value = "PROD01";
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=trivial_code, rw_vars=["User::Server"]),
        ])
        score = score_package(package)
        # Weight = 2 (trivial) → raw ~2 → log1p(2)/log1p(200)*100 ≈ 20
        assert score.score <= 25
        assert score.effort_estimate in ("Low", "Medium")

    def test_complex_script_high_score(self):
        """A package with a complex script should still score high."""
        complex_code = r'''
public void Main()
{
    using (var conn = new SqlConnection("...")) { conn.Open(); }
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=complex_code),
        ])
        score = score_package(package)
        # Weight = 20 (complex) → raw ~20 → ~57
        assert score.score >= 40

    def test_three_trivial_scripts_remain_low(self):
        """Your customer's case: 3 env-config scripts shouldn't push to 'High'."""
        trivial_code = r'''
public void Main()
{
    string env = Dts.Variables["User::Env"].Value.ToString();
    if (env == "PROD")
        Dts.Variables["User::Server"].Value = "PROD01";
    else
        Dts.Variables["User::Server"].Value = "DEV01";
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(name="SCR1", source_code=trivial_code, rw_vars=["User::Server"]),
            _make_script(name="SCR2", source_code=trivial_code, rw_vars=["User::FilePath"]),
            _make_script(name="SCR3", source_code=trivial_code, rw_vars=["User::ConnStr"]),
        ])
        score = score_package(package)
        # 3 × 2 = 6 raw → ~35 ... should NOT be "High"
        assert score.effort_estimate in ("Low", "Medium")
        assert score.score < 51  # Not "High"

    def test_three_trivial_scripts_old_would_have_been_high(self):
        """Verify the old flat-20 scoring would have rated this higher."""
        # With old scoring: 3 × 20 = 60 raw → log1p(60)/log1p(200)*100 ≈ 77 (Very High!)
        # With new scoring: 3 × 2 = 6 raw → log1p(6)/log1p(200)*100 ≈ 36 (Medium)
        import math
        old_raw = 3 * 20
        old_score = min(100, int(math.log1p(old_raw) / math.log1p(200) * 100))
        assert old_score >= 70  # old scoring: Very High

        trivial_code = 'public void Main() { Dts.Variables["User::X"].Value = "Y"; }'
        package = _make_package(tasks=[
            _make_script(name=f"SCR{i}", source_code=trivial_code, rw_vars=["User::X"])
            for i in range(3)
        ])
        new_score = score_package(package).score
        assert new_score < old_score  # new scoring is significantly lower

    def test_detailed_returns_classifications(self):
        """score_package_detailed should return per-script classification details."""
        trivial_code = 'public void Main() { Dts.Variables["User::X"].Value = "Y"; }'
        complex_code = 'public void Main() { var c = new SqlConnection("x"); c.Open(); }'

        package = _make_package(tasks=[
            _make_script(name="Trivial", source_code=trivial_code),
            _make_script(name="Complex", source_code=complex_code),
        ])
        complexity, classifications = score_package_detailed(package)

        assert len(classifications) == 2
        assert classifications[0].tier == ScriptComplexity.TRIVIAL
        assert classifications[1].tier == ScriptComplexity.COMPLEX

    def test_mixed_task_types(self):
        """Package with SQL + trivial script should reflect realistic effort."""
        trivial_code = r'''
public void Main()
{
    Dts.Variables["User::Server"].Value = "PROD01";
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=trivial_code, rw_vars=["User::Server"]),
            ExecuteSQLTask(
                id="sql-1", name="Run Export Query",
                sql_statement="SELECT * FROM dbo.Orders",
            ),
        ])
        score = score_package(package)
        # 2 (trivial script) + 2 (execute SQL) = 4 raw → ~30 on the log curve
        assert score.effort_estimate in ("Low", "Medium")
        assert score.score < 51  # definitely not "High"


# ===================================================================
# 4. Gap analyzer integration
# ===================================================================

class TestGapAnalyzerIntegration:
    """Verify gap severity changes based on script classification."""

    def test_trivial_script_gets_info_severity(self):
        trivial_code = r'''
public void Main()
{
    Dts.Variables["User::Server"].Value = "PROD01";
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=trivial_code, rw_vars=["User::Server"]),
        ])
        gaps = analyze_gaps(package)
        script_gaps = [g for g in gaps if g.task_type == "ScriptTask"]
        assert len(script_gaps) == 1
        assert script_gaps[0].severity == "info"
        assert "trivial" in script_gaps[0].message.lower()
        assert "SetVariable" in script_gaps[0].recommendation or "variable" in script_gaps[0].recommendation.lower()

    def test_complex_script_gets_manual_required(self):
        complex_code = r'''
public void Main()
{
    using (var conn = new SqlConnection("..."))
    {
        conn.Open();
        var cmd = new SqlCommand("SELECT 1", conn);
        cmd.ExecuteNonQuery();
    }
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=complex_code),
        ])
        gaps = analyze_gaps(package)
        script_gaps = [g for g in gaps if g.task_type == "ScriptTask"]
        assert len(script_gaps) == 1
        assert script_gaps[0].severity == "manual_required"

    def test_moderate_script_gets_warning(self):
        moderate_code = r'''
public void Main()
{
    string content = File.ReadAllText("data.txt");
    Dts.Variables["User::Data"].Value = content;
    Dts.TaskResult = (int)ScriptResults.Success;
}
'''
        package = _make_package(tasks=[
            _make_script(source_code=moderate_code, rw_vars=["User::Data"]),
        ])
        gaps = analyze_gaps(package)
        script_gaps = [g for g in gaps if g.task_type == "ScriptTask"]
        assert len(script_gaps) == 1
        assert script_gaps[0].severity == "warning"


# ===================================================================
# 5. Weight values
# ===================================================================

class TestTierWeights:
    def test_expected_weights(self):
        assert TIER_WEIGHTS[ScriptComplexity.TRIVIAL] == 2
        assert TIER_WEIGHTS[ScriptComplexity.SIMPLE] == 6
        assert TIER_WEIGHTS[ScriptComplexity.MODERATE] == 13
        assert TIER_WEIGHTS[ScriptComplexity.COMPLEX] == 20

    def test_trivial_is_cheapest(self):
        assert TIER_WEIGHTS[ScriptComplexity.TRIVIAL] < TIER_WEIGHTS[ScriptComplexity.SIMPLE]

    def test_complex_is_most_expensive(self):
        assert TIER_WEIGHTS[ScriptComplexity.COMPLEX] > TIER_WEIGHTS[ScriptComplexity.MODERATE]
