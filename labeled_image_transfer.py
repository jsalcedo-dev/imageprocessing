import os
import csv
import shutil
import itertools
import re
from typing import Iterable, Set, Dict, List, Optional

LIKELY_FILENAME_COLUMNS = {"filename"}

def _normalize(name: str, case_insensitive: bool = True) -> str:
    n = name.strip().strip('"').strip("'")
    # If the CSV contains paths, drop directories and keep the base filename
    n = os.path.basename(n)
    return n.lower() if case_insensitive else n

def _csv_name_to_tif(basename: str) -> str:
    """
    Convert a Roboflow-like export name to the original basename with .tif extension.

    Handles patterns like:
      <stem>_<jpg|jpeg|png|tif|tiff>.rf.<hash>.<ext>  ->  <stem>.tif

    Fallbacks:
      - If the above pattern isn't present but ".jpg" exists, take everything
        before the FIRST occurrence of ".jpg" and use .tif
      - Else, simply swap the file extension to .tif
    """
    base = os.path.basename(basename)
    lower = base.lower()

    # Case 1: Roboflow pattern: <stem>_<ext>.rf.<hash>.<ext>
    m = re.search(r"_(jpg|jpeg|png|tif|tiff)\.rf\.", lower)
    if m:
        stem = base[:m.start()]
        return stem + ".tif"

    # Case 2: Split at first ".jpg" if present anywhere
    idx = lower.find(".jpg")
    if idx != -1:
        stem = base[:idx]
        return stem + ".tif"

    # Case 3: Generic "replace extension with .tif"
    stem, _ = os.path.splitext(base)
    return stem + ".tif"

def load_filenames_from_csv(
    csv_path: str,
    column: Optional[str] = None,
    case_insensitive: bool = True,
) -> Set[str]:
    """
    Load a set of raw CSV filenames (normalized to basename + optional lowercase).
    """
    targets: Set[str] = set()

    # Try to sniff header; fall back gracefully
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = True  # best-effort default

        if has_header:
            reader = csv.DictReader(f, dialect=dialect)
            fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]
            use_col = None

            if column and column in fieldnames:
                use_col = column
            else:
                # pick the first "likely" filename-ish column
                for cand in fieldnames:
                    if cand.lower() in LIKELY_FILENAME_COLUMNS:
                        use_col = cand
                        break
                if use_col is None and fieldnames:
                    use_col = fieldnames[0]

            if not use_col:
                raise ValueError("Could not determine which CSV column contains filenames.")

            for row in reader:
                raw = row.get(use_col, "") or ""
                if raw.strip():
                    targets.add(_normalize(raw, case_insensitive))
        else:
            rdr = csv.reader(f, dialect=dialect)
            for row in rdr:
                if not row:
                    continue
                raw = row[0]
                if raw.strip():
                    targets.add(_normalize(raw, case_insensitive))

    return targets

def build_filename_index(
    search_path: str, case_insensitive: bool = True
) -> Dict[str, List[str]]:
    """
    Walk `search_path` once and build a map: normalized_basename -> [full_paths...]
    Supports multiple files with the same basename in different directories.
    """
    index: Dict[str, List[str]] = {}
    for root, _, files in os.walk(search_path):
        for file in files:
            key = file.lower() if case_insensitive else file
            index.setdefault(key, []).append(os.path.join(root, file))
    return index

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def copy_files(
    targets: Iterable[str],
    index: Dict[str, List[str]],
    destination_path: str,
    *,
    overwrite: bool = False,
    keep_dir_structure: bool = False,
    search_root_for_structure: Optional[str] = None,
) -> int:
    """
    Copy matching files to `destination_path`.

    - If `keep_dir_structure=True`, we replicate each file’s directory tree relative to
      `search_root_for_structure` (you should pass the same path you used to index).
    - If `overwrite=False`, name conflicts get a suffix: `_1`, `_2`, ...
    """
    ensure_dir(destination_path)
    copied = 0

    for target in targets:
        # Primary: look for .tif (already lowercased target)
        matches = index.get(target, [])

        # Graceful fallback: if not found and looks like .tif, also try .tiff
        if not matches and target.endswith(".tif"):
            matches = index.get(target[:-4] + ".tiff", [])

        if not matches:
            print(f"Not found: {target}")
            continue

        for src in matches:
            if keep_dir_structure and search_root_for_structure:
                rel_dir = os.path.relpath(os.path.dirname(src), search_root_for_structure)
                dest_dir = os.path.join(destination_path, rel_dir)
                ensure_dir(dest_dir)
                proposed = os.path.join(dest_dir, os.path.basename(src))
            else:
                dest_dir = destination_path
                ensure_dir(dest_dir)
                proposed = os.path.join(dest_dir, os.path.basename(src))

            dest_path = proposed
            if not overwrite and os.path.exists(dest_path):
                base, ext = os.path.splitext(os.path.basename(src))
                for i in itertools.count(1):
                    candidate = os.path.join(dest_dir, f"{base}_{i}{ext}")
                    if not os.path.exists(candidate):
                        dest_path = candidate
                        break

            try:
                shutil.copy2(src, dest_path)
                print(f"Copied '{src}' -> '{dest_path}'")
                copied += 1
            except IOError as e:
                print(f"Error copying '{src}': {e}")

    return copied

def find_and_copy_from_csv(
    csv_path: str,
    search_path: str,
    destination_path: str,
    *,
    column: Optional[str] = None,
    case_insensitive: bool = True,
    overwrite: bool = False,
    keep_dir_structure: bool = False,
):
    """
    End-to-end: read filenames from CSV, transform each to <stem>.tif,
    find them under `search_path`, and copy to `destination_path`.
    """
    print(f"Reading filenames from CSV: {csv_path}")
    raw_targets = load_filenames_from_csv(csv_path, column=column, case_insensitive=case_insensitive)
    if not raw_targets:
        print("No filenames loaded from CSV — nothing to do.")
        return

    # Transform CSV names -> .tif basenames
    tif_targets = set()
    for t in raw_targets:
        tif_name = _csv_name_to_tif(t)
        tif_targets.add(tif_name.lower() if case_insensitive else tif_name)

    print(f"Indexing files under: {search_path}")
    index = build_filename_index(search_path, case_insensitive=case_insensitive)

    print(f"Will look for {len(tif_targets)} .tif name(s). Copying into: {destination_path}")
    copied = copy_files(
        tif_targets,
        index,
        destination_path,
        overwrite=overwrite,
        keep_dir_structure=keep_dir_structure,
        search_root_for_structure=search_path,
    )

    if copied:
        print(f"\n✅ Successfully copied {copied} file(s) to '{destination_path}'.")
    else:
        print(f"\n❌ No matching files were found to copy.")

if __name__ == "__main__":
    # Example usage — adjust these paths as needed.
    csv_with_filenames = r"C:\Users\jesus\Downloads\Foreign Object Detection.v7-fo-10-4-25.tensorflow\train\_annotations.csv"
    starting_folder = r"G:\Supherb Drone Images"
    new_directory = r"G:\Supherb Drone Images\Labeled Tif Images"

    # If your CSV has a known column name, set `column="filename"` (or similar).
    find_and_copy_from_csv(
        csv_path=csv_with_filenames,
        search_path=starting_folder,
        destination_path=new_directory,
        column=None,                # or "filename", "path", etc.
        case_insensitive=True,      # set False if you need exact case matching
        overwrite=False,            # set True to overwrite instead of suffixing
        keep_dir_structure=False,   # set True to mirror source folders in destination
    )
