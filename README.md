# supconvert

Tonemapping and BDN XML export for PGS (`.sup`) subtitles.

## Requirements

```
pip install numpy pillow tqdm
```

## Usage

```
supconvert [input] [output] [MODES ...] [GAMMA ...] [OPTIONS ...] [VERBOSITY ...]
```

Output is auto-named if omitted. Input can be a single `.sup` file or a folder of `.sup` files.

## Tonemapping Modes

Multiple targets can be passed to a single mode to produce multiple outputs in one run.

| Flag | Description |
|---|---|
| `-p`, `--percent` | Target brightness as a percentage |
| `-cv`, `--rgb` | Target brightness as an 8-bit RGB code value |
| `-n`, `--nits` | Target brightness in nits |
| `-h`, `--hdr` | Convert SDR BT.709 to HDR BT.2020 PQ. Optional nits target (default: 203) |
| `-r`, `--ref` | Match the peak brightness of a reference `.sup` file |
| `-c`, `--lut` | Apply a 17, 33, or 65 point `.cube` 3D LUT |

If no mode or other flags are specified, supconvert defaults to the HDR paper white standard of 58% (~200nits). All brightness mapping modes display nits measurements under 68% (~500nits). (--nits overrides threshold) The HDR pipeline follows the SDR to HDR conversion formula outlined in BT.2408 §5.1.2.

## Gamma

Combinable with any tonemapping mode.

| Flag | Description |
|---|---|
| `-g`, `--gamma` | Apply manual gamma correction. Below 1.0 darkens, above 1.0 brightens |
| `-tb`, `--true-black` | Auto-calculates gamma to push grey outlines toward true black (default strength: 0.8) |

## Options

| Flag | Description |
|---|---|
| `-x`, `--xml` | Export to BDN XML + PNG image sequence |
| `-1`, `--first` | Export only the first graphic as a PNG |
| `-f`, `--force` | Skip overwrite warnings |

`-x` and `-1` are mutually exclusive. Both can be combined with a tonemapping mode to process then export in one pass. FPS is auto-detected from subtitle timing but can be overridden by passing a value to `-x`. A warning is shown if palette animations (e.g. fades) are detected, as BDN XML does not support them. Manually providing an output path with an `.xml` extension implicitly triggers `-x`.

## Verbosity

| Flag | Description |
|---|---|
| `-e`, `--exact` | Display percentages at full precision |
| `-l`, `--log` | Log each palette in tonemapping modes. Save full segment log with `--xml` or standalone |
| `-v`, `--verbose` | Like `--log` but printed to the CLI. Can be combined with `--log` |
| `-q`, `--quiet` | Suppress all output except the progress bar and warnings |
| `-s`, `--silent` | Suppress all output except warnings |

Due to the coarseness of the 16-235 limited range, multiple targets can round to the same displayed percentage. `--exact` makes those distinctions visible.

## Examples

```bash
# Tonemap to default 58%
supconvert input.sup

# Multiple RGB targets with true-black
supconvert input.sup --rgb 148 200 -tb

# Gamma only
supconvert input.sup -g 0.8

# Export to BDN XML
supconvert input.sup -x

# Tonemap then export to BDN XML
supconvert input.sup -p 60 -x

# Tonemap then export first graphic
supconvert input.sup -p 60 --first

# Match brightness of a reference file
supconvert input.sup -r reference.sup

# Apply a LUT to all .sup files in a folder
supconvert /path/to/folder -c my_lut.cube
