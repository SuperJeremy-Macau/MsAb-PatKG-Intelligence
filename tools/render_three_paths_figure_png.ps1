Add-Type -AssemblyName System.Drawing

$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root "docs\generated_ppt_assets"
$pngPath = Join-Path $outDir "three_current_paths.png"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$width = 2000
$height = 1180
$bmp = New-Object System.Drawing.Bitmap $width, $height
$g = [System.Drawing.Graphics]::FromImage($bmp)
$g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
$g.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
$g.Clear([System.Drawing.ColorTranslator]::FromHtml("#F6F8FB"))

function New-Brush([string]$hex) {
    return New-Object System.Drawing.SolidBrush([System.Drawing.ColorTranslator]::FromHtml($hex))
}

function New-PenObj([string]$hex, [float]$width) {
    return New-Object System.Drawing.Pen([System.Drawing.ColorTranslator]::FromHtml($hex), $width)
}

function Draw-RoundRect([System.Drawing.Graphics]$gr, [System.Drawing.Rectangle]$rect, [int]$radius, [string]$fillHex, [string]$lineHex) {
    $path = New-Object System.Drawing.Drawing2D.GraphicsPath
    $diameter = $radius * 2
    $arc = New-Object System.Drawing.Rectangle($rect.X, $rect.Y, $diameter, $diameter)
    $path.AddArc($arc, 180, 90)
    $arc.X = $rect.Right - $diameter
    $path.AddArc($arc, 270, 90)
    $arc.Y = $rect.Bottom - $diameter
    $path.AddArc($arc, 0, 90)
    $arc.X = $rect.X
    $path.AddArc($arc, 90, 90)
    $path.CloseFigure()
    $fill = New-Brush $fillHex
    $pen = New-PenObj $lineHex 3
    $gr.FillPath($fill, $path)
    $gr.DrawPath($pen, $path)
    $fill.Dispose()
    $pen.Dispose()
    $path.Dispose()
}

function Draw-Arrow([System.Drawing.Graphics]$gr, [int]$x1, [int]$y1, [int]$x2, [int]$y2) {
    $pen = New-PenObj "#6E7F99" 8
    $gr.DrawLine($pen, $x1, $y1, $x2, $y2)
    $pen.Dispose()
    $pts = New-Object 'System.Drawing.Point[]' 3
    $pts[0] = New-Object System.Drawing.Point($x2, $y2)
    $pts[1] = New-Object System.Drawing.Point(($x2 - 36), ($y2 - 18))
    $pts[2] = New-Object System.Drawing.Point(($x2 - 36), ($y2 + 18))
    $brush = New-Brush "#6E7F99"
    $gr.FillPolygon($brush, $pts)
    $brush.Dispose()
}

function Draw-Lines([System.Drawing.Graphics]$gr, [string[]]$lines, [System.Drawing.Font]$font, [string]$hex, [float]$x, [float]$y, [float]$gap) {
    $brush = New-Brush $hex
    $curY = $y
    foreach ($line in $lines) {
        $gr.DrawString($line, $font, $brush, $x, $curY)
        $size = $gr.MeasureString($line, $font)
        $curY += $size.Height + $gap
    }
    $brush.Dispose()
    return $curY
}

$fontTitle = New-Object System.Drawing.Font("Microsoft YaHei", 28, [System.Drawing.FontStyle]::Bold)
$fontSubtitle = New-Object System.Drawing.Font("Times New Roman", 16, [System.Drawing.FontStyle]::Regular)
$fontBoxTitle = New-Object System.Drawing.Font("Times New Roman", 18, [System.Drawing.FontStyle]::Bold)
$fontBody = New-Object System.Drawing.Font("Microsoft YaHei", 14, [System.Drawing.FontStyle]::Regular)
$fontSmall = New-Object System.Drawing.Font("Times New Roman", 12, [System.Drawing.FontStyle]::Regular)
$fontCall = New-Object System.Drawing.Font("Microsoft YaHei", 17, [System.Drawing.FontStyle]::Bold)

$navy = "#1D2B44"
$muted = "#4F607A"
$navyBrush = New-Brush $navy
$mutedBrush = New-Brush $muted

$g.DrawString("Three current paths from patent landscape to patent intelligence", $fontTitle, $navyBrush, 72, 54)
$g.DrawString("Shared data layer -> three reasoning routes -> evidence-grounded answer and benchmark", $fontSubtitle, $mutedBrush, 74, 114)

$boxes = @{
    data   = @{ Rect = (New-Object System.Drawing.Rectangle(70, 170, 400, 760)); Fill = "#DCEAFB"; Line = "#8FA9CF" }
    auto   = @{ Rect = (New-Object System.Drawing.Rectangle(560, 240, 420, 520)); Fill = "#FFF0BF"; Line = "#D1B870" }
    hybrid = @{ Rect = (New-Object System.Drawing.Rectangle(1020, 240, 420, 520)); Fill = "#DDF4E4"; Line = "#8CB89A" }
    frame  = @{ Rect = (New-Object System.Drawing.Rectangle(1480, 240, 420, 520)); Fill = "#F6D9EA"; Line = "#C89DB8" }
    output = @{ Rect = (New-Object System.Drawing.Rectangle(700, 865, 570, 195)); Fill = "#E4D9FA"; Line = "#A693D2" }
    call   = @{ Rect = (New-Object System.Drawing.Rectangle(1295, 840, 595, 240)); Fill = "#FFE4CB"; Line = "#D7A26E" }
}

foreach ($key in $boxes.Keys) {
    Draw-RoundRect $g $boxes[$key].Rect 28 $boxes[$key].Fill $boxes[$key].Line
}

Draw-Arrow $g 470 500 540 500
Draw-Arrow $g 980 500 1010 500
Draw-Arrow $g 1440 500 1470 500
Draw-Arrow $g 770 760 930 850
Draw-Arrow $g 1230 760 1080 850
Draw-Arrow $g 1680 760 1585 840

[void](Draw-Lines $g @("Shared data layer") $fontBoxTitle $navy 102 228 6)
[void](Draw-Lines $g @(
    "Curated patent dataset",
    "Neo4j KG + NodeCatalog",
    "Target / TargetPair / Assignee / Origin",
    "Pathway / Function / TechnologyClass1",
    "Intent JSON + benchmark datasets"
) $fontBody $navy 102 283 8)
[void](Draw-Lines $g @(
    "source of truth:",
    "tests/scripts + _curated_20260401"
) $fontSmall $muted 102 545 6)

[void](Draw-Lines $g @("Path A  AutoCypher") $fontBoxTitle $navy 590 290 6)
[void](Draw-Lines $g @(
    "official Text2Cypher",
    "v1 candidate ranking",
    "v2 one-shot baseline",
    "v3 repair with execution feedback"
) $fontBody $navy 590 345 8)
[void](Draw-Lines $g @(
    "role: baseline / fallback / ablation",
    "Dataset5: 143 match | 183 partial | 22 mismatch | 2 no_cypher"
) $fontSmall $muted 590 555 6)

[void](Draw-Lines $g @("Path B  Hybrid Intent") $fontBoxTitle $navy 1050 290 6)
[void](Draw-Lines $g @(
    "intent classification",
    "entity extraction + normalization",
    "template Cypher first",
    "AutoCypher fallback when needed"
) $fontBody $navy 1050 345 8)
[void](Draw-Lines $g @(
    "role: current deployable path",
    "Dataset5: 115 match | 198 partial | 32 mismatch | 5 no_cypher"
) $fontSmall $muted 1050 555 6)

[void](Draw-Lines $g @("Path C  Query Frame few-shot") $fontBoxTitle $navy 1510 290 6)
[void](Draw-Lines $g @(
    "select FRAME_* question pattern",
    "slot filling",
    "execute fixed Cypher frame",
    "answer from graph results"
) $fontBody $navy 1510 345 8)
[void](Draw-Lines $g @(
    "role: current best benchmark path",
    "Dataset5: 216 match | 126 partial | 8 mismatch"
) $fontSmall $muted 1510 555 6)

[void](Draw-Lines $g @("Unified output and evaluation") $fontBoxTitle $navy 735 920 6)
[void](Draw-Lines $g @(
    "KG-grounded answer",
    "Cypher + graph evidence",
    "audit workbook",
    "match / partial / mismatch / no_cypher"
) $fontBody $navy 735 975 8)

[void](Draw-Lines $g @("Current takeaway") $fontCall $navy 1330 888 6)
[void](Draw-Lines $g @(
    "Query Frame is the strongest benchmark path.",
    "Hybrid is the most maintainable deployable path.",
    "AutoCypher remains necessary as baseline and fallback."
) $fontBody $navy 1330 940 10)

$bmp.Save($pngPath, [System.Drawing.Imaging.ImageFormat]::Png)

$fontTitle.Dispose()
$fontSubtitle.Dispose()
$fontBoxTitle.Dispose()
$fontBody.Dispose()
$fontSmall.Dispose()
$fontCall.Dispose()
$navyBrush.Dispose()
$mutedBrush.Dispose()
$g.Dispose()
$bmp.Dispose()

Write-Output "Saved PNG: $pngPath"
