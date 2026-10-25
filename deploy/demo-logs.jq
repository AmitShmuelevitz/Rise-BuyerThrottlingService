# Condenses evaluator JSON logs into one aligned line per decision.
def pad($n): tostring | if length < $n then . + (" " * ($n - length)) else . end;

if .buyer then
  "\(.time[11:19]) \(.msg | pad(37)) \(.buyer | pad(16)) rate \("\(.rate_from)->\(.rate_to)" | pad(10)) \(.reason | pad(12)) n=\(.total | pad(5)) fail=\(.failure_rate | pad(5)) timeout=\(.timeout_rate)"
else
  "\(.time[11:19]) \(.msg) \(.owner)"
end
