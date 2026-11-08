# Terminal recording for the README, rendered with vhs (https://github.com/charmbracelet/vhs):
#   vhs docs/demo.tape      writes docs/demo.gif
# Run it from the repository root with datacom on PATH.

Output docs/demo.gif
Set Shell bash
Set FontSize 15
Set Width 1100
Set Height 640
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Type "head -5 testdata/messy.csv"
Enter
Sleep 2.5s

Type "datacom clean testdata/messy.csv -snake -dedupe -fold-case -phone 254 -monitor"
Enter
Sleep 6s

Type "datacom query testdata/messy.csv -snake -where 'amount_kes > 1000' -group region -agg 'count,sum(amount_kes)' -sort -sum_amount_kes"
Enter
Sleep 5s
