"""
Script Task content classifier — analyzes the actual content of SSIS Script Tasks
to assign an accurate complexity tier rather than a flat weight.

Classification tiers
--------------------
- **trivial**  (weight  2): Only assigns variable values, typically for environment-based
  configuration.  Maps directly to ADF variables / parameters / expressions.
- **simple**   (weight  6): Light string / path manipulation, environment lookups,
  basic branching.  Expressible as ADF expressions or Set Variable activities.
- **moderate** (weight 13): File I/O, regex, basic HTTP, XML/JSON parsing.  Needs an
  Azure Function but the port is straightforward.
- **complex**  (weight 25): Database connections, COM interop, threading, external
  libraries, heavy business logic.  Full manual effort required.

When ``source_code`` is available the classifier inspects the C# / VB source text.
When it is not (binary-compressed scripts), it falls back to heuristic analysis of
the declared variable names and counts.
"""
from __future__ import annotations

import re
from enum import Enum

from ..parsers.models import ScriptTask

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class ScriptComplexity(str, Enum):
    TRIVIAL = "trivial"
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


# Weight assigned to each tier in the complexity scorer
TIER_WEIGHTS: dict[ScriptComplexity, int] = {
    ScriptComplexity.TRIVIAL: 2,
    ScriptComplexity.SIMPLE: 6,
    ScriptComplexity.MODERATE: 13,
    ScriptComplexity.COMPLEX: 25,
}


class ScriptClassificationResult:
    """Outcome of classifying a single Script Task."""

    __slots__ = ("tier", "weight", "reason", "variables_only", "adf_expressible")

    def __init__(
        self,
        tier: ScriptComplexity,
        reason: str,
        *,
        variables_only: bool = False,
        adf_expressible: bool = False,
    ) -> None:
        self.tier = tier
        self.weight = TIER_WEIGHTS[tier]
        self.reason = reason
        self.variables_only = variables_only
        self.adf_expressible = adf_expressible


# ---------------------------------------------------------------------------
# Regex patterns used by source-code analysis
# ---------------------------------------------------------------------------

# Complex indicators — any single match ⇒ complex
_COMPLEX_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:SqlConnection|OleDbConnection|SqlCommand|OleDbCommand|SqlDataAdapter)\b", re.I),
     "Uses direct database connections"),
    (re.compile(r"\bType\.GetTypeFromProgID\b", re.I),
     "Uses COM interop"),
    (re.compile(r"\bMarshal\b", re.I),
     "Uses COM / P-Invoke marshalling"),
    (re.compile(r"\b(?:Thread|Task\.Run|Parallel\.For|Parallel\.ForEach|ThreadPool)\b"),
     "Uses threading / parallel execution"),
    (re.compile(r"\breflection\b|\.GetType\(\)\.GetMethod\(|Activator\.CreateInstance", re.I),
     "Uses reflection"),
    (re.compile(r"\b(?:SmtpClient|MailMessage)\b"),
     "Uses SMTP / email sending"),
    (re.compile(r"\b(?:Process\.Start|ProcessStartInfo)\b"),
     "Launches external processes"),
    (re.compile(r"\b(?:Assembly\.Load|AppDomain)\b"),
     "Dynamic assembly loading"),
    (re.compile(r"\b(?:RegistryKey|Registry\.)\b"),
     "Windows Registry access"),
    (re.compile(r"\b(?:Encrypt|Decrypt|RSA|AES|SHA256|MD5|CryptoStream)\b", re.I),
     "Uses cryptography APIs"),
    (re.compile(r"\b(?:ServicePointManager|TlsVersion)\b"),
     "Network security configuration"),
]

# Moderate indicators — file I/O, regex, HTTP, XML/JSON parsing
_MODERATE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bFile\.(?:Read|Write|AppendAll|Copy|Move|Delete|Exists|Open)"),
     "Uses file I/O operations"),
    (re.compile(r"\bDirectory\.(?:GetFiles|GetDirectories|Create|Delete|Exists|EnumerateFiles)"),
     "Uses directory operations"),
    (re.compile(r"\bStreamReader|StreamWriter|FileStream\b"),
     "Uses stream-based file I/O"),
    (re.compile(r"\bRegex\b"),
     "Uses regular expressions"),
    (re.compile(r"\b(?:HttpClient|WebClient|WebRequest|HttpWebRequest)\b"),
     "Uses HTTP client calls"),
    (re.compile(r"\b(?:XDocument|XmlDocument|XmlReader|XmlWriter|XElement)\b"),
     "Uses XML parsing/writing"),
    (re.compile(r"\b(?:JsonConvert|JObject|JArray|JsonSerializer|JsonDocument)\b"),
     "Uses JSON parsing"),
    (re.compile(r"\b(?:DataTable|DataSet|DataRow)\b"),
     "Uses in-memory data tables"),
]

# Simple indicators — string/path logic, environment lookups
_SIMPLE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bPath\.Combine\b"),
     "Uses path construction"),
    (re.compile(r"\bEnvironment\.GetEnvironmentVariable\b"),
     "Reads environment variables"),
    (re.compile(r"\bConfigurationManager\b"),
     "Reads configuration settings"),
    (re.compile(r"\bString\.Format\b"),
     "Uses string formatting"),
    (re.compile(r"\b(?:\.Replace\(|\.Substring\(|\.Split\(|\.ToUpper\(|\.ToLower\(|\.Trim\()"),
     "Uses string manipulation"),
    (re.compile(r"\bDateTime\.(?:Now|Today|UtcNow|Parse|ParseExact)\b"),
     "Uses date/time operations"),
    (re.compile(r"\bConvert\.To(?:Int32|String|DateTime|Boolean|Double)\b"),
     "Uses type conversions"),
]

# Trivial: only Dts.Variables assignments with simple right-hand sides
_DTS_VARIABLE_ASSIGN = re.compile(
    r"""Dts\.Variables\s*\[\s*["'].*?["']\s*\]\.Value\s*=""",
    re.DOTALL,
)
_DTS_VARIABLE_READ = re.compile(
    r"""Dts\.Variables\s*\[\s*["'].*?["']\s*\]\.Value""",
    re.DOTALL,
)

# Lines that are irrelevant to complexity (blanks, comments, using, namespace/class boilerplate)
_BOILERPLATE_LINE = re.compile(
    r"^\s*$"
    r"|^\s*//"
    r"|^\s*/?\*"
    r"|^\s*using\s"
    r"|^\s*namespace\s"
    r"|^\s*\[Microsoft\.SqlServer"
    r"|^\s*\[System\.AddIn"
    r"|^\s*public\s+(?:partial\s+)?class\s"
    r"|^\s*(?:public|private|internal)\s+(?:enum|struct)\s"
    r"|^\s*#(?:region|endregion)"
    r"|^\s*\{"
    r"|^\s*\}"
    r"|^\s*(?:readonly|const)\s"
    r"|^\s*(?:ScriptResults\.Success|ScriptResults\.Failure|Dts\.TaskResult)"
    r"|^\s*(?:bool\s+fire|Dts\.Events\.Fire)",
)

# Variable names that strongly suggest config / environment usage
_CONFIG_VARIABLE_NAMES = re.compile(
    r"(?:server|database|catalog|connection|conn|env|environment|instance|"
    r"filepath|file_?path|folder|directory|dir|config|setting|"
    r"source|destination|target|url|uri|endpoint|host|port|"
    r"schema|user|username|password|secret|key|tenant|subscription)",
    re.I,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_script(task: ScriptTask) -> ScriptClassificationResult:
    """Classify a Script Task's actual complexity based on its content."""
    if task.source_code:
        return _classify_from_source(task)
    return _classify_from_heuristics(task)


# ---------------------------------------------------------------------------
# Source-code analysis path
# ---------------------------------------------------------------------------

def _classify_from_source(task: ScriptTask) -> ScriptClassificationResult:
    """Inspect actual C# / VB source code to determine complexity tier."""
    code = task.source_code or ""

    # --- Check complex patterns first (highest priority) ---
    for pattern, reason in _COMPLEX_PATTERNS:
        if pattern.search(code):
            return ScriptClassificationResult(
                ScriptComplexity.COMPLEX,
                reason,
            )

    # --- Check if code is trivially just variable assignments ---
    if _is_trivial_variable_assignment(code):
        return ScriptClassificationResult(
            ScriptComplexity.TRIVIAL,
            "Script only assigns variable values — replaceable with ADF variables/parameters",
            variables_only=True,
            adf_expressible=True,
        )

    # --- Check moderate patterns ---
    moderate_reasons: list[str] = []
    for pattern, reason in _MODERATE_PATTERNS:
        if pattern.search(code):
            moderate_reasons.append(reason)

    if moderate_reasons:
        return ScriptClassificationResult(
            ScriptComplexity.MODERATE,
            "; ".join(moderate_reasons),
        )

    # --- Check simple patterns ---
    simple_reasons: list[str] = []
    for pattern, reason in _SIMPLE_PATTERNS:
        if pattern.search(code):
            simple_reasons.append(reason)

    if simple_reasons:
        # Check if still ADF-expressible (string ops + simple branching)
        meaningful_lines = _count_meaningful_lines(code)
        adf_expressible = meaningful_lines <= 20
        return ScriptClassificationResult(
            ScriptComplexity.SIMPLE,
            "; ".join(simple_reasons),
            adf_expressible=adf_expressible,
        )

    # --- Fallback: judge by code volume ---
    meaningful_lines = _count_meaningful_lines(code)
    if meaningful_lines <= 5:
        return ScriptClassificationResult(
            ScriptComplexity.TRIVIAL,
            f"Script has only {meaningful_lines} meaningful line(s) of code",
            variables_only=False,
            adf_expressible=True,
        )
    if meaningful_lines <= 15:
        return ScriptClassificationResult(
            ScriptComplexity.SIMPLE,
            f"Script has {meaningful_lines} meaningful lines with no complex API usage",
            adf_expressible=meaningful_lines <= 10,
        )
    if meaningful_lines <= 50:
        return ScriptClassificationResult(
            ScriptComplexity.MODERATE,
            f"Script has {meaningful_lines} meaningful lines of unrecognised logic",
        )
    return ScriptClassificationResult(
        ScriptComplexity.COMPLEX,
        f"Script has {meaningful_lines} meaningful lines — substantial logic likely requires manual porting",
    )


def _is_trivial_variable_assignment(code: str) -> bool:
    """
    Return True when the script body does nothing beyond:
      - reading Dts.Variables
      - assigning to Dts.Variables
      - setting Dts.TaskResult
      - simple if/else/switch for environment branching

    This covers the extremely common SSIS pattern of "pick connection strings
    based on environment" or "set file paths for dev vs. prod".
    """
    meaningful = _extract_meaningful_lines(code)
    if not meaningful:
        return True  # empty script body ⇒ trivial

    for line in meaningful:
        # Allow Dts.Variables[...].Value = ... (read or write)
        if _DTS_VARIABLE_ASSIGN.search(line):
            continue

        # Allow Dts.TaskResult = ...
        if re.match(r"\s*Dts\.TaskResult\s*=", line):
            continue

        # Allow simple control flow: if / else if / else / switch / case / break / return
        if re.match(
            r"\s*(?:if|else\s+if|else|switch|case|default|break|return)\b", line
        ):
            continue

        # Allow simple local variable declarations with Dts.Variables reads
        # e.g.  string env = Dts.Variables["User::Environment"].Value.ToString();
        #       var server = (string)Dts.Variables["User::Server"].Value;
        if re.match(r"\s*(?:var|string|int|bool|object|double|decimal)\s+\w+\s*=", line):
            # RHS must only reference Dts.Variables, literals, or the ternary operator
            rhs = line.split("=", 1)[1] if "=" in line else ""
            if _rhs_is_simple(rhs):
                continue

        # Allow standalone Dts.Variables reads (sometimes used in conditions)
        if _DTS_VARIABLE_READ.search(line) and not _has_method_call_beyond_basics(line):
            continue

        # Allow .ToString() calls on their own line
        if re.match(r"\s*\.\s*ToString\s*\(", line):
            continue

        # Anything else ⇒ not trivial
        return False

    return True


def _rhs_is_simple(rhs: str) -> bool:
    """Check that a right-hand side only uses simple constructs."""
    cleaned = rhs.strip().rstrip(";").strip()
    # Allow: Dts.Variables reads, string literals, numeric literals, bool literals,
    #        ternary operator (?:), comparison operators, parentheses, casts, .ToString()
    # Disallow: method calls (except .ToString, .Trim, .Value), new, constructor calls
    if re.search(r"\bnew\s+\w+", cleaned):
        return False
    if _has_method_call_beyond_basics(cleaned):
        return False
    return True


def _has_method_call_beyond_basics(text: str) -> bool:
    """Return True if text contains method calls beyond basic safe ones."""
    safe_methods = {
        "ToString", "Trim", "TrimStart", "TrimEnd",
        "ToUpper", "ToLower", "Equals",
        "Value",  # Dts.Variables[...].Value
    }
    # Find all .MethodName( patterns
    for m in re.finditer(r"\.(\w+)\s*\(", text):
        if m.group(1) not in safe_methods:
            return True
    return False


def _extract_meaningful_lines(code: str) -> list[str]:
    """Return lines that are not boilerplate."""
    result: list[str] = []
    in_main = False
    brace_depth = 0

    for line in code.splitlines():
        # Try to focus on the Main() method body
        if re.search(r"\bvoid\s+Main\s*\(", line):
            in_main = True
            brace_depth = 0
            continue

        if in_main:
            brace_depth += line.count("{") - line.count("}")
            if brace_depth <= 0 and "{" not in line and "}" in line:
                in_main = False
                continue

        if not in_main:
            # If we never found Main(), fall through below
            continue

        if _BOILERPLATE_LINE.match(line):
            continue

        result.append(line)

    # If Main() was never found, analyse all non-boilerplate lines
    if not result:
        for line in code.splitlines():
            if _BOILERPLATE_LINE.match(line):
                continue
            # Skip class/method declarations outside Main
            if re.match(r"\s*(?:public|private|protected|internal)\s+", line):
                if "Main" not in line and "=" not in line:
                    continue
            result.append(line)

    return result


def _count_meaningful_lines(code: str) -> int:
    return len(_extract_meaningful_lines(code))


# ---------------------------------------------------------------------------
# Heuristic path (no source code available)
# ---------------------------------------------------------------------------

def _classify_from_heuristics(task: ScriptTask) -> ScriptClassificationResult:
    """
    Best-effort classification when source code is not extractable.

    Uses variable names and counts as proxies for what the script likely does.
    """
    all_vars = task.read_only_variables + task.read_write_variables
    rw_vars = task.read_write_variables

    if not all_vars:
        return ScriptClassificationResult(
            ScriptComplexity.COMPLEX,
            "No source code available and no declared variables — cannot infer purpose; assuming complex",
        )

    # Count how many variables have "config-like" names
    config_count = sum(1 for v in all_vars if _CONFIG_VARIABLE_NAMES.search(v))
    config_ratio = config_count / len(all_vars) if all_vars else 0

    # Only writes variables, no read-only inputs, few variables ⇒ likely trivial config
    if (
        len(task.read_only_variables) <= 2
        and len(rw_vars) <= 8
        and config_ratio >= 0.6
    ):
        return ScriptClassificationResult(
            ScriptComplexity.TRIVIAL,
            f"No source code, but {config_count}/{len(all_vars)} variables have config-like names "
            f"(e.g. Server, FilePath, Environment) — likely environment variable assignment",
            variables_only=True,
            adf_expressible=True,
        )

    if config_ratio >= 0.4 and len(all_vars) <= 12:
        return ScriptClassificationResult(
            ScriptComplexity.SIMPLE,
            f"No source code; {config_count}/{len(all_vars)} variables are config-like — "
            f"likely simple configuration logic",
            adf_expressible=True,
        )

    # Many variables or few config-like names ⇒ we can't be sure, err on the side of moderate
    if len(all_vars) <= 6:
        return ScriptClassificationResult(
            ScriptComplexity.MODERATE,
            f"No source code; {len(all_vars)} variables with unclear purpose — "
            f"assuming moderate complexity",
        )

    return ScriptClassificationResult(
        ScriptComplexity.COMPLEX,
        f"No source code; {len(all_vars)} variables — assuming complex due to uncertainty",
    )
