# Real Axware exports

Captured from <https://results.solo2.com> (Cal Club) and kept verbatim as parser
regression fixtures. Each pair covers something the parsers have to handle.

| Files | Why it is here |
| --- | --- |
| `04-03-2022-*` | Oldest layout. Class header is 9 wide (`Team, S/T, Factor, Run 1..4, Total`) over 13-wide rows, so `Total` lands at index 12. The PAX sheet is 11 wide with `#` at index 2. This is the event `national_avg.sh` was written for. |
| `01-25-2026-*` | Class header `Car Color, Run 1..4, Total, Diff.` puts `Total` at index 10. PAX sheet is 11 wide but with an extra `Pos.` column, moving `#` to index 3. The page header carries **no date** — it is only in the filename. |
| `04-25-2026-*` | The top three CS finishers are 600-series cars, which Cal Club does not award points to. Their times still set the benchmark the scoring drivers are curved against. Same on the PAX sheet, where car 647 wins outright. |
| `08-16-2026-*` | Class header `S/T, Factor, Run 1..4, Total` puts `Total` last, at index 11. PAX sheet is the older 9-wide shape. |

Three different class layouts and three different PAX layouts across four events,
with `Total` landing at index 10, 11 or 12 depending on the export. That is why
the parsers resolve columns by name from each table's header row instead of
counting in from either end.

`season-index-2026.html` is the season listing from
<https://results.solo2.com/index.php?dir=2026>, used to test index discovery.
