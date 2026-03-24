import os, struct, argparse, sys, io
import numpy as np
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, as_completed
from PIL import Image
import xml.etree.ElementTree as ET
from xml.dom import minidom

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# --- ST 2084 / PQ Constants ---
PQ_M1 = 2610.0 / 16384.0; PQ_M2 = (2523.0 / 4096.0) * 128.0; PQ_C1 = 3424.0 / 4096.0; PQ_C2 = (2413.0 / 4096.0) * 32.0; PQ_C3 = (2392.0 / 4096.0) * 32.0

# --- PGS Segment Constants ---
SYNC = b"PG"; SEG_PALETTE = 0x14; SEG_OBJECT = 0x15; SEG_PRESENTATION = 0x16; SEG_END = 0x80
SEG_NAMES = {0x14: "PALETTE", 0x15: "OBJECT ", 0x16: "PRESENT", 0x80: "END    "}

# --- Palette Analysis ---

def get_palette_stats(payload_or_path):
    """Returns (rep_dark, peak_y_cv).
    rep_dark: alpha-weighted average Y of dark entries (normalised 0–1), used for true-black gamma calc.
    peak_y_cv: highest visible Y code value (limited range integer).
    Accepts either a palette payload bytearray or a .sup file path (scans to first palette segment)."""
    if isinstance(payload_or_path, (str, os.PathLike)):
        if not os.path.exists(payload_or_path): return (0.0, None)
        with open(payload_or_path, "rb") as f: data = f.read()
        curr = 0
        while curr < len(data):
            h = data[curr:curr+13]
            if len(h) < 13: break
            _, _, _, stype, ssize = struct.unpack(">2sIIBH", h)
            if stype == SEG_PALETTE:
                payload_or_path = bytearray(data[curr+13 : curr+13+ssize])
                break
            curr += 13 + ssize
        else:
            return (0.0, None)
    payload = payload_or_path
    entries = []
    peak_y_cv = 16
    peak_val = 0.0
    for i in range(2, len(payload)-4, 5):
        alpha = payload[i+4]
        if alpha > 0:
            y = payload[i+1]
            val = (y - 16) / 219.0
            if val > peak_val:
                peak_val = val
                peak_y_cv = y
            entries.append((val, alpha))
    if not entries: return (0.0, 16)
    dark_entries = [(v, a) for v, a in entries if v <= peak_val * 0.6]
    if dark_entries:
        sum_val = sum(v * a for v, a in dark_entries)
        sum_alpha = sum(a for v, a in dark_entries)
        rep_dark = sum_val / sum_alpha if sum_alpha > 0 else 0.0
    else:
        rep_dark = 0.0
    return rep_dark, peak_y_cv

# --- Color Conversion ---

def conv_ycbcr_to_rgb(y, cb, cr):
    """8-bit limited YCbCr → normalised 0–1 RGB tuple."""
    y_n = (float(y) - 16.0) / 219.0
    cb_n = (float(cb) - 128.0) / 224.0
    cr_n = (float(cr) - 128.0) / 224.0
    r = y_n + 1.5748 * cr_n
    g = y_n - 0.1873 * cb_n - 0.4681 * cr_n
    b = y_n + 1.8556 * cb_n
    return (max(0.0, min(1.0, r)), max(0.0, min(1.0, g)), max(0.0, min(1.0, b)))

def conv_rgb_to_ycbcr(rgb):
    """Normalised 0–1 RGB → 8-bit limited YCbCr tuple."""
    r, g, b = (max(0.0, min(1.0, v)) for v in rgb)
    y  = 0.2126 * r + 0.7152 * g + 0.0722 * b
    cb = (b - y) / 1.8556
    cr = (r - y) / 1.5748
    y_out, cb_out, cr_out = 16.0 + 219.0 * y, 128.0 + 224.0 * cb, 128.0 + 224.0 * cr
    return (max(0, min(255, int(round(y_out)))),
            max(0, min(255, int(round(cr_out)))),
            max(0, min(255, int(round(cb_out)))))

# --- Tonemapping ---

def apply_transform_to_payload(payload, multiplier, gamma):
    """Scale each palette entry's Y by multiplier in limited range space.
    If gamma is set, applies power curve to normalised Y before scaling."""
    new_payload = bytearray(payload)
    use_gamma = gamma is not None and gamma != 1.0
    for i in range(2, len(new_payload)-4, 5):
        y = new_payload[i+1]
        y_norm = (y - 16) / 219.0
        if use_gamma:
            y_norm = y_norm ** (1.0 / gamma)
        new_payload[i+1] = max(16, min(235, int(round(16.0 + y_norm * multiplier * 219.0))))
    return new_payload

def verify_tonemap(payload, target_cv, warn):
    """Checks the peak Y of a processed palette against the intended target. Warns if they differ."""
    _, actual_cv = get_palette_stats(payload)
    delta = actual_cv - target_cv
    if delta != 0:
        warn(f"Warning: Tonemap delta {delta:+d} (target Y:{target_cv}, got Y:{actual_cv})")

# --- Nits / HDR / PQ ---

def nits_to_y_cv(nits):
    """Converts nits to an 8-bit limited range Y code value via ST 2084 PQ."""
    L = np.clip(nits / 10000.0, 0.0, 1.0)
    V = ((PQ_C1 + PQ_C2 * (L ** PQ_M1)) / (1.0 + PQ_C3 * (L ** PQ_M1))) ** PQ_M2
    return max(16, min(235, int(round(16.0 + V * 219.0))))

def y_cv_to_nits(y_cv):
    """Converts an 8-bit limited range Y code value to nits via ST 2084 PQ."""
    V = np.clip((y_cv - 16.0) / 219.0, 0.0, 1.0)
    V_1_m2 = V ** (1.0 / PQ_M2)
    num = max(0.0, V_1_m2 - PQ_C1)
    den = PQ_C2 - PQ_C3 * V_1_m2
    L = (num / den) ** (1.0 / PQ_M1) if den > 0 else 0.0
    return L * 10000.0

def apply_hdr_to_payload(payload, target_nits, gamma=None):
    """BT.2408 §5.1.2 display-referred SDR→PQ per palette entry.
    Pipeline: limited YCbCr → linear RGB → optional gamma → BT.1886 EOTF → BT.2087 M2 → PQ → limited YCbCr."""
    # BT.2087 Annex 1 M2 — BT.709 to BT.2020 linear RGB matrix (NPM_2020^-1 × NPM_709)
    M_709_2020 = np.array([
        [0.62739819, 0.32930480, 0.04332235],
        [0.06904461, 0.91957770, 0.01137611],
        [0.01636178, 0.08799233, 0.89555168],
    ])
    L_scale = target_nits / 10000.0
    new_payload = bytearray(payload)
    use_gamma = gamma is not None and gamma != 1.0
    for i in range(2, len(new_payload)-4, 5):
        y, cr, cb = new_payload[i+1], new_payload[i+2], new_payload[i+3]
        rgb = np.array(conv_ycbcr_to_rgb(y, cb, cr))
        if use_gamma:
            rgb = np.power(rgb, 1.0 / gamma)
        rgb_linear = np.power(rgb, 2.40)                                   # BT.1886 EOTF
        rgb_linear = np.clip(M_709_2020 @ rgb_linear, 0.0, 1.0)            # BT.2087 M2 gamut map
        L = np.clip(rgb_linear * L_scale, 0.0, 1.0)                        # scale to absolute luminance
        Lm1 = L ** PQ_M1
        rgb_pq = ((PQ_C1 + PQ_C2 * Lm1) / (1.0 + PQ_C3 * Lm1)) ** PQ_M2    # ST 2084 PQ encode
        ny, ncr, ncb = conv_rgb_to_ycbcr(rgb_pq)
        new_payload[i+1], new_payload[i+2], new_payload[i+3] = ny, ncr, ncb
    return new_payload

# --- LUT ---

def parse_cube_file(filepath):
    """Parses a 17, 33, or 65 point .cube LUT file into a float32 numpy array."""
    if not filepath.lower().endswith('.cube'):
        raise ValueError("LUT must be a .cube file.")
    with open(filepath, 'r') as f:
        lines = f.readlines()
    size, data = 0, []
    for line in lines:
        line = line.strip()
        if line.startswith('LUT_3D_SIZE'):
            size = int(line.split()[1])
        elif line and not line.startswith('#') and not line.isalpha() and not line.startswith('LUT'):
            parts = line.split()
            if len(parts) == 3:
                data.append([float(x) for x in parts])
    if size == 0 or len(data) != size**3:
        raise ValueError(f"Invalid .cube file. Expected {size**3} entries for size {size}, found {len(data)}.")
    return np.array(data, dtype=np.float32).reshape((size, size, size, 3))

class LUT:
    def __init__(self, data):
        self.d = data
        self.max_idx = float(data.shape[0] - 1)

    def apply(self, r, g, b):
        rf, gf, bf = r * self.max_idx, g * self.max_idx, b * self.max_idx
        r0, g0, b0 = int(rf), int(gf), int(bf)
        r1 = min(r0+1, int(self.max_idx)); g1 = min(g0+1, int(self.max_idx)); b1 = min(b0+1, int(self.max_idx))
        dr, dg, db = rf-r0, gf-g0, bf-b0
        c000 = self.d[b0,g0,r0]; c100 = self.d[b0,g0,r1]; c010 = self.d[b0,g1,r0]; c001 = self.d[b1,g0,r0]
        c110 = self.d[b0,g1,r1]; c101 = self.d[b1,g0,r1]; c011 = self.d[b1,g1,r0]; c111 = self.d[b1,g1,r1]
        return (c000*(1-dr)*(1-dg)*(1-db) + c100*dr*(1-dg)*(1-db) +
                c010*(1-dr)*dg*(1-db) + c001*(1-dr)*(1-dg)*db +
                c110*dr*dg*(1-db) + c101*dr*(1-dg)*db +
                c011*(1-dr)*dg*db + c111*dr*dg*db)

def apply_lut_to_payload(payload, lut):
    """Applies a 3D LUT to all palette entries via trilinear interpolation."""
    new_payload = bytearray(payload)
    for i in range(2, len(new_payload)-4, 5):
        y, cr, cb = new_payload[i+1], new_payload[i+2], new_payload[i+3]
        rgb_out = lut.apply(*conv_ycbcr_to_rgb(y, cb, cr))
        ny, ncr, ncb = conv_rgb_to_ycbcr(rgb_out)
        new_payload[i+1], new_payload[i+2], new_payload[i+3] = ny, ncr, ncb
    return new_payload

# --- Processing ---

def process_palette_group(task):
    """Applies the active transform to all palette segments in one display set group."""
    group_data, multiplier, gamma, hdr_nits, lut_data = task
    lut = LUT(lut_data) if lut_data is not None else None
    output, offset = bytearray(), 0
    while offset < len(group_data):
        h = group_data[offset:offset+13]
        if len(h) < 13: break
        _, _, _, stype, ssize = struct.unpack(">2sIIBH", h)
        payload = bytearray(group_data[offset+13 : offset+13+ssize])
        if stype == SEG_PALETTE:
            if lut is not None:
                payload = apply_lut_to_payload(payload, lut)
            elif hdr_nits is not None:
                payload = apply_hdr_to_payload(payload, hdr_nits, gamma)
            elif multiplier is not None:
                payload = apply_transform_to_payload(payload, multiplier, gamma)
        elif stype == SEG_PRESENTATION and len(payload) >= 5:
            payload[4] = (payload[4] & 0xF0) | 0x02  # mark palette as updated
        output.extend(h); output.extend(payload)
        offset += 13 + ssize
    return output

def process_sup_file(input_path, output_arg, mode, target_val, gamma, true_black, verbose, silent=False, quiet=False, is_last=False, log=None, warn=None, force=False, return_bytes=False, log_path=None, precise=False, first_only=False):
    if log is None:  log  = lambda *a, **kw: None if (silent or quiet) else print(*a, **kw)
    if warn is None: warn = print
    with open(input_path, "rb") as f: data = f.read()

    palette_groups, current_group, first_payload = [], bytearray(), None
    curr, has_seen_palette = 0, False
    while curr < len(data):
        h = data[curr:curr+13]
        if len(h) < 13: break
        _, _, _, stype, ssize = struct.unpack(">2sIIBH", h)
        if stype == SEG_PALETTE:
            if not first_payload: first_payload = bytearray(data[curr+13 : curr+13+ssize])
            if has_seen_palette:
                palette_groups.append(bytes(current_group)); current_group = bytearray()
            has_seen_palette = True
        current_group.extend(data[curr:curr+13+ssize]); curr += 13+ssize
    if current_group: palette_groups.append(bytes(current_group))
    if first_only: palette_groups = palette_groups[:1]

    if not first_payload:
        warn(f"Error: No valid palette payloads found in {input_path}.")
        return None

    if true_black:
        rep_dark, peak_y_cv = get_palette_stats(first_payload)
        if rep_dark > 0.005:
            target_dark = max(rep_dark * (1.0 - true_black), 0.001)
            E = np.log(target_dark) / np.log(rep_dark)
            gamma = 1.0 / E
            rep_dark_y   = max(16, min(235, int(round(16 + rep_dark * 219))))
            target_dark_y = max(16, min(235, int(round(16 + target_dark * 219))))
            log(f"--- True-Black Mode ---")
            log(f"Grey outline detected at Y: {rep_dark_y}")
            log(f"Targeting new outline Y: {target_dark_y} (Darken Amount: {true_black})")
            log(f"Auto-calculated Gamma to achieve true-black: {gamma:.4f}\n")
        else:
            log("--- True-Black Mode ---")
            log("No significant grey outline found. Skipping True-Black.\n")
        current_cv = peak_y_cv
    else:
        _, current_cv = get_palette_stats(first_payload)

    if mode in ('passthrough', 'lut'):
        final_multiplier = 1.0 if (mode == 'passthrough' and gamma is not None and gamma != 1.0) else None
        target_cv = current_cv
        if mode == 'passthrough' and gamma is not None and gamma != 1.0:
            log(f"--- Applying Gamma ({gamma:.4f}) ---\n")
        elif mode == 'lut':
            lut_name = os.path.splitext(os.path.basename(target_val))[0]
            log(f"--- Applying LUT: {os.path.basename(target_val)} ---\n")

    elif mode == 'hdr':
        target_cv = nits_to_y_cv(target_val)
        compliant_percent = (target_cv - 16) / 219.0 * 100.0
        final_multiplier = None
        t_rgb = round((target_cv - 16) / 219.0 * 255.0)
        perc_str = f"{compliant_percent}%" if precise else f"{compliant_percent:.0f}%"
        log(f"--- SDR to HDR Conversion ---")
        log(f"PQ Target: {target_val:.0f} nits | {perc_str} | Y:{target_cv} | RGB:{t_rgb}")
        if gamma is not None and gamma != 1.0:
            log(f"Gamma: {gamma:.4f}")
        log()

    else:
        source_percent = (current_cv - 16) / 219.0 * 100.0
        if mode == 'percent':
            orig_target_cv = max(16, min(235, int(round(16.0 + (target_val / 100.0) * 219.0))))
        elif mode == 'rgb':
            orig_target_cv = max(16, min(235, int(round(16.0 + (np.clip(target_val, 0, 255) / 255.0) * 219.0))))
        elif mode == 'nits':
            orig_target_cv = nits_to_y_cv(target_val)
        elif mode == 'ref':
            orig_target_cv = get_palette_stats(target_val)[1]
            if orig_target_cv is None:
                warn(f"Error: Could not extract target brightness from reference file {target_val}")
                return None

        if current_cv == orig_target_cv:
            warn(f"Warning: {os.path.basename(input_path)} source brightness already matches target. Skipping.")
            return None

        if gamma is not None and gamma != 1.0:
            log(f"--- Tonemapping to Target Brightness (Gamma: {gamma:.4f}) ---")
        else:
            log(f"--- Tonemapping to Target Brightness ---")

        # Without gamma: (target-16)/(source-16). With gamma: pre-warp source via power curve first.
        if gamma is not None and gamma != 1.0:
            seed_y_norm = ((current_cv - 16) / 219.0) ** (1.0 / gamma)
            final_multiplier = (orig_target_cv - 16) / (seed_y_norm * 219.0) if seed_y_norm > 0 else 1.0
        else:
            final_multiplier = (orig_target_cv - 16) / (current_cv - 16) if current_cv > 16 else 1.0
        target_cv = orig_target_cv
        compliant_percent = (target_cv - 16) / 219.0 * 100.0

        hide_nits = (compliant_percent > 68.0) and (mode != 'nits')
        perc_fmt  = "" if precise else ".0f"
        nits_fmt  = lambda n: f"{n} nits" if n < 1 else f"{n:.0f} nits"
        t_perc  = f"{compliant_percent:{perc_fmt}}%"
        t_y     = f"Y:{target_cv}"
        t_rgb_v = round((target_cv - 16) / 219.0 * 255.0)
        t_rgb   = f"RGB:{t_rgb_v}"
        t_nits  = nits_fmt(y_cv_to_nits(target_cv))
        s_perc  = f"{source_percent:{perc_fmt}}%"
        s_y     = f"Y:{current_cv}"
        s_rgb_v = round((current_cv - 16) / 219.0 * 255.0)
        s_rgb   = f"RGB:{s_rgb_v}"
        s_nits  = nits_fmt(y_cv_to_nits(current_cv))
        if mode == 'rgb':
            s_str = f"{s_rgb} | {s_y} | {s_perc}" + (f" | {s_nits}" if not hide_nits else "")
            t_str = f"{t_rgb} | {t_y} | {t_perc}" + (f" | {t_nits}" if not hide_nits else "")
        elif mode == 'nits':
            s_str = f"{s_nits} | {s_perc} | {s_y} | {s_rgb}"
            t_str = f"{t_nits} | {t_perc} | {t_y} | {t_rgb}"
        else:
            s_str = f"{s_perc} | {s_y} | {s_rgb}" + (f" | {s_nits}" if not hide_nits else "")
            t_str = f"{t_perc} | {t_y} | {t_rgb}" + (f" | {t_nits}" if not hide_nits else "")
        log(f"Source: {s_str}")
        log(f"Target: {t_str}")
        log(f"Final Multiplier: {final_multiplier:.6f}")
        log()

    gamma_tag = f"_G{gamma:.2f}" if gamma is not None and gamma != 1.0 else ""
    if mode == 'rgb':          suffix = f"RGB{int(target_val)}{gamma_tag}"
    elif mode == 'nits':       suffix = f"{target_val:.0f}nits{gamma_tag}"
    elif mode == 'hdr':        suffix = f"HDR{target_val:.0f}{gamma_tag}"
    elif mode == 'lut':        suffix = lut_name
    elif mode == 'ref':        suffix = f"ref{compliant_percent:.0f}{gamma_tag}"
    elif mode == 'passthrough': suffix = gamma_tag.lstrip('_')
    else:                      suffix = f"{compliant_percent:.0f}{gamma_tag}"

    if not return_bytes:
        if output_arg:
            if not output_arg.lower().endswith('.sup'):
                os.makedirs(output_arg, exist_ok=True)
                base_name = f"{os.path.splitext(os.path.basename(input_path))[0]}_{suffix}.sup"
                out_path = os.path.join(output_arg, base_name)
            else:
                out_path = output_arg
        else:
            out_path = f"{os.path.splitext(input_path)[0]}_{suffix}.sup"

        if os.path.exists(out_path) and not force:
            warn(f"Warning: '{os.path.basename(out_path)}' already exists.")
            if input("Overwrite? (y/N): ").lower() != 'y':
                return None

    log(f"Modifying {len(palette_groups)} {'palette' if len(palette_groups) == 1 else 'palettes'}...")
    hdr_nits = target_val if mode == 'hdr' else None
    lut_data = parse_cube_file(target_val) if mode == 'lut' else None
    tasks = [(pg, final_multiplier, gamma, hdr_nits, lut_data) for pg in palette_groups]

    if verbose or log_path:
        log_file = open(log_path, "w", encoding="utf-8") if log_path else None
        processed = []
        for idx, task in enumerate(tasks, 1):
            pg = task[0]
            h = pg[:13]
            pts = struct.unpack(">I", h[2:6])[0] / 90000.0 if len(h) >= 13 else 0.0
            ssize = struct.unpack(">H", h[11:13])[0] if len(h) >= 13 else 0
            colors = (ssize - 2) // 5 if ssize >= 2 else 0
            line = f"  [{idx:4}/{len(tasks)}] PTS: {pts:10.3f} | Colors: {colors}"
            if verbose: log(line)
            if log_file: log_file.write(line + "\n")
            processed.append(process_palette_group(task))
        if log_file:
            log_file.close()
            log(f"Palette log saved to: {log_path}")
    else:
        if len(tasks) == 1:
            processed = [process_palette_group(tasks[0])]
        else:
            with Pool() as pool:
                if tqdm and not silent:
                    processed = list(tqdm(pool.imap(process_palette_group, tasks, chunksize=1),
                                     total=len(tasks), unit="pal",
                                     bar_format='{l_bar}{bar}| {percentage:3.0f}% [{rate_fmt}]'))
                else:
                    processed = list(pool.imap(process_palette_group, tasks))

    if final_multiplier is not None and final_multiplier != 1.0 and processed:
        # Extract the first palette payload from the first processed group for verification
        group = processed[0]
        h = group[:13]
        if len(h) == 13:
            _, _, _, stype, ssize = struct.unpack(">2sIIBH", h)
            if stype == SEG_PALETTE:
                verify_tonemap(bytearray(group[13:13+ssize]), target_cv, warn)

    if return_bytes:
        return b''.join(processed), suffix

    with open(out_path, "wb") as f_out:
        for r in processed: f_out.write(r)

    log(f"\nOutput saved to: {out_path}" + ("" if is_last else "\n" + "-"*40))
    return out_path


# --- XML / BDN Export ---

def read_pts(data):
    return struct.unpack(">I", data)[0] / 90000.0

def seconds_to_tc(seconds, fps):
    fps_int = int(round(fps))
    total_frames = int(round(seconds * fps))
    frames = total_frames % fps_int
    total_secs = total_frames // fps_int
    secs = total_secs % 60
    mins = (total_secs // 60) % 60
    hours = total_secs // 3600
    return f"{hours:02}:{mins:02}:{secs:02}:{frames:02}"

def decode_rle(width, height, data):
    pixels = []
    i, total_pixels = 0, width * height
    while len(pixels) < total_pixels and i < len(data):
        b = data[i]; i += 1
        if b != 0:
            pixels.append(b)
        else:
            if i >= len(data): break
            b2 = data[i]; i += 1
            if b2 == 0:
                line_rem = width - (len(pixels) % width)
                if line_rem < width: pixels.extend([0] * line_rem)
            else:
                flag = b2 & 0xC0
                if flag == 0x00:   length, color = b2 & 0x3F, 0
                elif flag == 0x40: length = ((b2 & 0x3F) << 8) | data[i]; i += 1; color = 0
                elif flag == 0x80: length = b2 & 0x3F; color = data[i]; i += 1
                else:              length = ((b2 & 0x3F) << 8) | data[i]; i += 1; color = data[i]; i += 1
                pixels.extend([color] * length)
    return pixels[:total_pixels]

def worker_task(task_data):
    obj_list, palette, out_path, canvas_w, canvas_h, min_x, min_y = task_data
    canvas = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
    for obj in obj_list:
        px = decode_rle(obj['w'], obj['h'], obj['data'])
        img = Image.new("RGBA", (obj['w'], obj['h']))
        img.putdata([palette.get(i, (0, 0, 0, 0)) for i in px])
        temp_layer = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
        temp_layer.paste(img, (obj['x'] - min_x, obj['y'] - min_y))
        canvas = Image.alpha_composite(canvas, temp_layer)
    canvas.save(out_path)
    return out_path

def handle_fps_logic(events, manual_fps, silent=False, quiet=False):
    def log(*a, **kw):
        if not silent and not quiet: print(*a, **kw)
    candidates = [23.976, 24.0, 25.0, 29.97, 50.0, 59.94]
    scores = {c: 0.0 for c in candidates}
    for ev in events:
        t = ev['start']
        for c in candidates: scores[c] += abs((t * c) - round(t * c))
    detected_fps = min(scores, key=scores.get)
    if manual_fps is None:
        log("-" * 45)
        log(f"FPS Auto-Detect: {detected_fps} FPS")
        log("-" * 45)
        return detected_fps
    if abs(detected_fps - manual_fps) < 0.001:
        log("-" * 45)
        log(f"Verified: Manual FPS ({manual_fps}) matches detected timing.")
        log("-" * 45)
        return manual_fps
    else:
        print("-" * 45)
        print(f"WARNING: Manual FPS ({manual_fps}) deviates from detected {detected_fps}!")
        choice = input(f"Are you sure you want to override the detected fps? (default: n/no): ").lower()
        if choice in ['y', 'yes']:
            log("-" * 45)
            log(f"Overriding with (more than likely incorrect) manual {manual_fps} FPS.")
            log("-" * 45)
            return manual_fps
        else:
            log(f"Proceeding with detected {detected_fps} FPS.")
            log("-" * 45)
            return detected_fps

class PGSParser:
    def __init__(self, filename, outdir, prefix, verbose=False, log=False, first_only=False, log_path=None, dry_run=False, source_data=None):
        self.filename = filename
        self.outdir = outdir
        self.prefix = prefix
        self.verbose = verbose
        self.log = log
        self.first_only = first_only
        self.log_path = log_path
        self.dry_run = dry_run
        self.source_data = source_data
        self.palette = {}
        self.objects = {}
        self.events = []
        self.img_counter = 0
        self.start_pts = 0
        self.current_comps = []
        if not dry_run and outdir and not os.path.exists(outdir):
            os.makedirs(outdir, exist_ok=True)

    def parse(self, executor=None):
        futures = []
        log_file = None
        if (self.verbose or self.log) and self.log_path:
            log_file = open(self.log_path, "w", encoding="utf-8")

        def v_print(msg):
            if self.verbose: print(msg)
            if log_file: log_file.write(msg + "\n")

        try:
            with (io.BytesIO(self.source_data) if self.source_data is not None else open(self.filename, "rb")) as f:
                while True:
                    header = f.read(13)
                    if len(header) < 13: break
                    if header[0:2] != SYNC: continue
                    pts = read_pts(header[2:6])
                    seg_type, seg_size = header[10], struct.unpack(">H", header[11:13])[0]
                    payload = f.read(seg_size)

                    if self.verbose or self.log:
                        s_name = SEG_NAMES.get(seg_type, f"UNK({hex(seg_type)})")
                        v_msg = f"[PTS: {pts:10.3f}] {s_name} | Size: {seg_size:6}"
                        if seg_type == SEG_PALETTE:
                            v_msg += f" | Colors: {(len(payload)-2)//5}"
                        elif seg_type == SEG_OBJECT:
                            oid = struct.unpack(">H", payload[0:2])[0]
                            v_msg += f" | ObjID: {oid}"
                        elif seg_type == SEG_PRESENTATION:
                            v_msg += f" | Comps: {payload[10] if len(payload) > 10 else 0}"
                        v_print(v_msg)

                    if self.dry_run:
                        continue

                    if seg_type == SEG_PALETTE:
                        for i in range(2, len(payload), 5):
                            idx, y, cr, cb, alpha = payload[i:i+5]
                            self.palette[idx] = (*tuple(int(round(c * 255)) for c in conv_ycbcr_to_rgb(y, cb, cr)), alpha)
                    elif seg_type == SEG_OBJECT:
                        obj_id = struct.unpack(">H", payload[0:2])[0]
                        if payload[3] & 0x80:
                            w, h = struct.unpack(">HH", payload[7:11])
                            self.objects[obj_id] = {'w': w, 'h': h, 'data': payload[11:]}
                        else:
                            if obj_id in self.objects:
                                self.objects[obj_id]['data'] += payload[4:]
                    elif seg_type == SEG_PRESENTATION:
                        if len(payload) >= 11:
                            if self.events and self.events[-1]['end'] == 0:
                                self.events[-1]['end'] = pts
                            num_comps = payload[10]
                            self.current_comps = []
                            pos = 11
                            for _ in range(num_comps):
                                if pos + 8 > len(payload): break
                                oid = struct.unpack(">H", payload[pos:pos+2])[0]
                                x, y = struct.unpack(">HH", payload[pos+4:pos+8])
                                self.current_comps.append({'id': oid, 'x': x, 'y': y})
                                pos += 8
                            if num_comps > 0:
                                self.start_pts = pts
                    elif seg_type == SEG_END:
                        if self.start_pts > 0 and self.current_comps:
                            valid_objs = []
                            for c in self.current_comps:
                                if c['id'] in self.objects:
                                    obj = self.objects[c['id']]
                                    valid_objs.append({'w': obj['w'], 'h': obj['h'], 'data': obj['data'], 'x': c['x'], 'y': c['y']})
                            if valid_objs:
                                self.img_counter += 1
                                fname = f"{self.prefix}_{self.img_counter:04}.png"
                                min_x = min(v['x'] for v in valid_objs)
                                min_y = min(v['y'] for v in valid_objs)
                                max_x = max(v['x'] + v['w'] for v in valid_objs)
                                max_y = max(v['y'] + v['h'] for v in valid_objs)
                                task = (valid_objs, self.palette.copy(), os.path.join(self.outdir, fname), max_x-min_x, max_y-min_y, min_x, min_y)
                                v_print(f"    >> Queuing Image {fname}: {max_x-min_x}x{max_y-min_y} at ({min_x},{min_y})")
                                futures.append(executor.submit(worker_task, task))
                                self.events.append({"file": fname, "start": self.start_pts, "end": 0, "x": min_x, "y": min_y, "w": max_x-min_x, "h": max_y-min_y})
                                self.start_pts = 0
                                if self.first_only: return futures
        finally:
            if log_file: log_file.close()
        return futures

    def detect_palette_animations(self):
        has_anim = False
        with open(self.filename, "rb") as f:
            in_sequence = False
            found_palette = False
            while True:
                header = f.read(13)
                if len(header) < 13: break
                seg_type = header[10]
                seg_size = struct.unpack(">H", header[11:13])[0]
                if seg_type == SEG_PRESENTATION:
                    in_sequence = True; found_palette = False
                elif seg_type == SEG_PALETTE and in_sequence:
                    found_palette = True
                elif seg_type == SEG_OBJECT:
                    in_sequence = False
                elif seg_type == SEG_END:
                    if in_sequence and found_palette:
                        has_anim = True; break
                    in_sequence = False
                f.seek(seg_size, 1)
        return has_anim

def write_xml(events, xml_dir, asset_subfolder, xml_name, fps):
    first_tc = seconds_to_tc(events[0]['start'], fps) if events else "00:00:00:00"
    last_tc = seconds_to_tc(events[-1]['end'], fps) if events else "00:00:00:00"
    container = ET.Element("Body")
    desc = ET.SubElement(container, "Description")
    ET.SubElement(desc, "Format", {"VideoFormat": "1080p", "FrameRate": str(fps), "DropFrame": "false"})
    ET.SubElement(desc, "Events", {"Type": "Graphic", "FirstEventInTC": first_tc, "LastEventOutTC": last_tc, "NumberofEvents": str(len(events))})
    events_list = ET.SubElement(container, "Events")
    for event in events:
        if event.get('end', 0) == 0: event['end'] = event['start'] + 2.0
        ev_node = ET.SubElement(events_list, "Event", {"InTC": seconds_to_tc(event['start'], fps), "OutTC": seconds_to_tc(event['end'], fps), "Forced": "false"})
        ET.SubElement(ev_node, "Graphic", {"Width": str(event.get('w', 0)), "Height": str(event.get('h', 0)), "X": str(event.get('x', 0)), "Y": str(event.get('y', 0)), "VideoFormat": "1080p"}).text = f"{asset_subfolder}/{event['file']}"
    inner_xml_chunks = []
    for child in container:
        raw_chunk = ET.tostring(child, encoding='unicode')
        try:
            pretty_chunk = minidom.parseString(raw_chunk).toprettyxml(indent="  ")
            pretty_chunk = "\n".join(pretty_chunk.split('\n')[1:])
            inner_xml_chunks.append(pretty_chunk)
        except:
            inner_xml_chunks.append(raw_chunk)
    xml_path = os.path.join(xml_dir, f"{xml_name}.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<BDN Version="0.93" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="BD-03-006-0093b BDN File Format.xsd">\n')
        f.write("".join(inner_xml_chunks))
        f.write('</BDN>')
    return xml_path

def run_first_export(sup_path, out_dir, log, source_data=None, suffix=None):
    """Exports only the first graphic from a .sup file as a PNG."""
    sup_abs = os.path.abspath(sup_path)
    sup_name = os.path.splitext(os.path.basename(sup_abs))[0]
    png_name = f"{sup_name}_{suffix}" if suffix else sup_name
    os.makedirs(out_dir, exist_ok=True)
    parser = PGSParser(sup_abs, out_dir, png_name, first_only=True, source_data=source_data)
    with ProcessPoolExecutor() as executor:
        futures = parser.parse(executor)
        if futures:
            saved_full_path = futures[0].result()
            log(f"First graphic saved as {os.path.basename(saved_full_path)}")

def run_xml_export(sup_path, xml_parent_dir, manual_fps, verbose, log_flag, force, log, warn, silent, quiet, source_data=None, suffix=None, xml_stem=None):
    sup_abs = os.path.abspath(sup_path)
    sup_dir = os.path.dirname(sup_abs)
    sup_name = os.path.splitext(os.path.basename(sup_abs))[0]
    xml_name = xml_stem if xml_stem else (f"{sup_name}_{suffix}" if suffix else sup_name)
    asset_subfolder_name = f"{xml_name}_images"
    image_outdir = os.path.join(xml_parent_dir, asset_subfolder_name)
    log_file_path = os.path.join(sup_dir, f"{xml_name}.log") if verbose or log_flag else None

    xml_path = os.path.join(xml_parent_dir, f"{xml_name}.xml")

    if os.path.exists(xml_path) and not force:
        warn(f"Warning: '{os.path.basename(xml_path)}' already exists.")
        if input("Overwrite? (y/N): ").lower() != 'y':
            return

    os.makedirs(image_outdir, exist_ok=True)
    parser = PGSParser(sup_abs, image_outdir, xml_name, verbose, log_flag, log_path=log_file_path, source_data=source_data)
    with ProcessPoolExecutor() as executor:
        futures = parser.parse(executor)
        if not verbose:
            log(f"Processing {len(futures)} subtitles...")
        for _ in (tqdm(as_completed(futures), total=len(futures), unit="sub", disable=verbose or silent) if tqdm else as_completed(futures)):
            pass

    active_fps = handle_fps_logic(parser.events, manual_fps, silent, quiet)
    has_anim = parser.detect_palette_animations()

    if has_anim:
        warn("🚨 PALETTE ANIMATIONS DETECTED! 🚨")
        warn("This sup contains rapid-fire color updates (most likely fades).")
        warn("The BDN XML format unfortunately does not have support for these.")
        warn("Since every change is now an individual PNG, the output")
        warn("will likely be SIGNIFICANTLY larger than the .sup!")
        warn("-" * 45)

    write_xml(parser.events, xml_parent_dir, asset_subfolder_name, xml_name, active_fps)
    log(f"\nXML saved to: {os.path.join(xml_parent_dir, xml_name + '.xml')}")
    log(f"PNGs saved to: {image_outdir}")
    if verbose or log_flag:
        log(f"Segment log saved to: {log_file_path}")


# --- CLI ---

class CleanHelpFormatter(argparse.RawDescriptionHelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=55, width=170)

def main():
    p = argparse.ArgumentParser(
        description="Tonemapping and BDN XML export for PGS subtitles.",
        add_help=False, formatter_class=CleanHelpFormatter,
        usage="%(prog)s \033[92m[input]\033[0m \033[92m[output]\033[0m \033[94m[MODES ...]\033[0m \033[94m[GAMMA ...]\033[0m \033[94m[OPTIONS ...]\033[0m \033[94m[VERBOSITY ...]\033[0m",
        epilog="examples:\n  supconvert input.sup                           tonemap to default 58%\n  supconvert input.sup --rgb 148 200 -tb         multiple RGB targets with true-black\n  supconvert input.sup -g 0.8                    gamma only\n  supconvert input.sup -x                        export to BDN XML\n  supconvert input.sup -p 60 -x                  tonemap then export to BDN XML\n  supconvert input.sup -p 60 --first             tonemap then export first graphic\n  supconvert input.sup -r reference.sup          match brightness of a reference file\n  supconvert /path/to/folder -c my_lut.cube      apply a LUT to all .sup files in a folder"
    )
    p.add_argument("--help", action="help", help=argparse.SUPPRESS)
    p.add_argument("input", nargs='?', help="Path to a .sup file or folder containing .sup files")
    p.add_argument("output", nargs='?', help="Output file or folder (optional, auto-named if omitted)")

    modes = p.add_argument_group("tonemapping/conversion modes").add_mutually_exclusive_group()
    modes.add_argument("-p", "--percent", type=float, nargs='+', metavar="0-100", help="Target brightness percentage(s) (Default: 58.0)")
    modes.add_argument("-cv", "--rgb", type=int, nargs='+', metavar="0-255", help="Target 8-bit full range RGB code value(s)")
    modes.add_argument("-n", "--nits", type=float, nargs='+', metavar="0-10000", help="Target nits value(s)")
    modes.add_argument("-h", "--hdr", type=float, nargs='*', metavar="0-10000", help="Convert SDR 709 to HDR 2020. Optional nits target(s) (Default if triggered: 203)")
    modes.add_argument("-r", "--ref", type=str, nargs='+', metavar="SUP", help="Match peak brightness of reference .sup file(s)")
    modes.add_argument("-c", "--lut", type=str, nargs='+', metavar="CUBE", help="Apply 17, 33, or 65 point .cube LUT(s)")

    gm = p.add_argument_group("gamma").add_mutually_exclusive_group()
    gm.add_argument("-g", "--gamma", type=float, metavar="0-10", help="Apply manual gamma correction. Values below 1.0 darken, above 1.0 brighten.")
    gm.add_argument("-tb", "--true-black", type=float, nargs='?', const=0.8, metavar="0.0-1.0", help="Darken grey outlines closer to black. Auto-calculates gamma internally. (Default if triggered: 0.8)")

    og = p.add_argument_group("options")
    xml_group = og.add_mutually_exclusive_group()
    xml_group.add_argument("-x", "--xml", type=float, nargs='?', const=0.0, metavar="FPS",
                    help="Export to BDN XML + PNG image sequence. Optional FPS (auto-detected by default).")
    xml_group.add_argument("-1", "--first", action="store_true", help="Export only the first graphic as a PNG.")
    og.add_argument("-f", "--force", action="store_true", help="Skip overwrite warnings.")

    vg = p.add_argument_group("verbosity")
    vg.add_argument("-e", "--exact", action="store_true", help="Display percentages at full precision.")
    vg.add_argument("-l", "--log", action="store_true", help="Log each palette in tonemapping modes. Save full segment log with --xml or standalone.")
    vg_out = vg.add_mutually_exclusive_group()
    vg_out.add_argument("-v", "--verbose", action="store_true", help="Like --log but printed to the CLI. Can be combined with --log.")
    vg_out.add_argument("-q", "--quiet", action="store_true", help="Suppress all output except the progress bar and warnings.")
    vg_out.add_argument("-s", "--silent", action="store_true", help="Suppress all output except warnings.")
    args = p.parse_args()

    if len(sys.argv) == 1:
        p.print_help(); sys.exit()

    if not os.path.exists(args.input):
        print(f"Error: The input path '{args.input}' does not exist.")
        return

    def log(*a, **kw):
        if not args.silent and not args.quiet: print(*a, **kw)
    def warn(*a, **kw):
        print(*a, **kw)

    input_abs = os.path.abspath(args.input)
    input_dir = os.path.dirname(input_abs) if not os.path.isdir(input_abs) else input_abs
    srcs = [os.path.join(input_abs, f) for f in os.listdir(input_abs) if f.lower().endswith('.sup')] if os.path.isdir(input_abs) else [input_abs]

    # Detect .xml output — treat as implicit --xml with named output
    xml_output_stem = None
    if args.output and args.output.lower().endswith('.xml'):
        xml_output_stem = os.path.splitext(os.path.basename(args.output))[0]
        xml_output_dir  = os.path.dirname(os.path.abspath(args.output)) or input_dir
        if args.xml is None:
            args.xml = 0.0  # trigger xml mode with auto-detect
        args.output = None  # don't pass .xml path as a sup output dir
    else:
        xml_output_dir = None

    gamma_active = args.gamma or args.true_black
    use_default = not gamma_active and not (args.xml is not None or args.first or args.log or args.verbose)
    hdr_vals = args.hdr if args.hdr else ([203.0] if args.hdr is not None else None)
    targets = [('ref', x) for x in args.ref] if args.ref else \
              [('lut', x) for x in args.lut] if args.lut else \
              [('hdr', x) for x in hdr_vals] if hdr_vals is not None else \
              [('rgb', x) for x in args.rgb] if args.rgb else \
              [('nits', x) for x in args.nits] if args.nits else \
              [('percent', x) for x in (args.percent or ([58.0] if use_default else []))] or \
              ([('passthrough', None)] if gamma_active else [])

    if (args.log or args.verbose) and args.xml is None and not args.first and not targets:
        for src in srcs:
            src_name = os.path.splitext(os.path.basename(src))[0]
            src_log = os.path.join(os.path.dirname(src), f"{src_name}.log") if args.log else None
            parser = PGSParser(src, None, None, verbose=args.verbose, log=args.log, log_path=src_log, dry_run=True)
            parser.parse()
            if src_log:
                print(f"Segment log saved to: {src_log}")

    xml_inputs = []

    xml_active = args.xml is not None or args.first

    if args.output and args.output.lower().endswith('.sup'):
        if os.path.isdir(args.input):
            print("Error: Cannot use a .sup output path when input is a folder.")
            return

    if args.xml is None or targets:
        for m, v in targets:
            if m == 'ref':
                ref_y = get_palette_stats(v)[1]
                ref_perc = f"{(ref_y - 16) / 219.0 * 100.0:.0f}" if ref_y is not None else "ref"
                tag_val = f"ref{ref_perc}"
            else:
                tag_val = f"RGB{v}" if m == 'rgb' else f"{v:.0f}nits" if m == 'nits' else f"HDR{v:.0f}" if m == 'hdr' else f"{os.path.splitext(os.path.basename(v))[0]}" if m == 'lut' else f"{v:.0f}" if m == 'percent' else ""
            gamma_tag = f"_G{args.gamma}" if args.gamma is not None and args.gamma != 1.0 else ""

            out_path = args.output
            if out_path and out_path.lower().endswith('.sup') and len(targets) > 1:
                out_path = f"{os.path.splitext(out_path)[0]}_{tag_val}{gamma_tag}.sup"

            if os.path.isdir(args.input):
                base = out_path or args.input
                if len(targets) > 1:
                    out_path = os.path.join(base, f"supconvert_{tag_val}{gamma_tag}")
                elif not out_path:
                    out_path = os.path.join(args.input, f"supconvert_{tag_val}{gamma_tag}")
                os.makedirs(out_path, exist_ok=True)

            for i, s in enumerate(srcs):
                src_log = os.path.join(os.path.dirname(s), f"{os.path.splitext(os.path.basename(s))[0]}.log") if args.log else None
                result = process_sup_file(s, out_path, m, v, args.gamma, args.true_black, args.verbose, args.silent, args.quiet, is_last=(i == len(srcs) - 1), log=log, warn=warn, force=args.force, return_bytes=xml_active, log_path=src_log, precise=args.exact, first_only=args.first)
                if result is not None and xml_active:
                    sup_bytes, sup_suffix = result
                    xml_inputs.append((s, sup_bytes, sup_suffix))

    if args.first:
        out_dir = xml_output_dir or args.output or input_dir
        if xml_inputs:
            for src_path, sup_bytes, sup_suffix in xml_inputs:
                run_first_export(src_path, out_dir, log, source_data=sup_bytes, suffix=sup_suffix)
        else:
            for sup in srcs:
                run_first_export(sup, out_dir, log)
        sys.exit(0)

    if args.xml is not None:
        manual_fps = args.xml if args.xml else None
        xml_parent_dir = xml_output_dir or args.output or input_dir

        if xml_inputs:
            for src_path, sup_bytes, sup_suffix in xml_inputs:
                run_xml_export(src_path, xml_parent_dir, manual_fps, args.verbose, args.log, args.force, log, warn, args.silent, args.quiet, source_data=sup_bytes, suffix=sup_suffix, xml_stem=xml_output_stem)
        else:
            for sup in srcs:
                run_xml_export(sup, xml_parent_dir, manual_fps, args.verbose, args.log, args.force, log, warn, args.silent, args.quiet, xml_stem=xml_output_stem)

if __name__ == "__main__":
    main()
