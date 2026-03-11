import json
import argparse
import sys
import traceback
from pathlib import Path

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_occurrence(start_ms, end_ms, confidence=100.0):
    """
    AssemblyAI timestamps are typically returned in milliseconds.
    SDNA requires them in seconds (with 2 decimal precision).
    """
    if start_ms is None: start_ms = 0
    if end_ms is None: end_ms = 0
    if confidence is None: confidence = 0.0
    
    try:
        return {
            "confidence_score": round(float(confidence), 2),
            "start": round(float(start_ms) / 1000.0, 2),
            "end": round(float(end_ms) / 1000.0, 2)
        }
    except (ValueError, TypeError):
        return {
            "confidence_score": 0.0,
            "start": 0.0,
            "end": 0.0
        }

def sort_by_timestamp(data_list):
    if not data_list:
        return []
    try:
        return sorted(
            data_list,
            key=lambda x: x["occurrences"][0]["start"] if (x.get("occurrences") and len(x["occurrences"]) > 0) else float('inf')
        )
    except Exception:
        return data_list

def normalize_transcript(data, target_language=None):
    """
    Attempts to pull diarized utterances first. Falls back to base text if missing.
    If target_language is provided, attempts to bridge to translated_texts.
    """
    if not isinstance(data, dict):
        return []
        
    utterances = data.get("utterances")
    if not isinstance(utterances, list) or not utterances:
        text = data.get("text") or ""
        if not text:
            return []
        # Fallback to translation if top level text is not translated but utterances are missing
        # (Usually AssemblyAI puts the translation in the top level 'text' for translated jobs,
        # but if utterances exist, we prefer them for timing).
        duration = float(data.get("audio_duration") or 0) * 1000 
        return [{
            "value": text,
            "occurrences": [build_occurrence(0, duration)]
        }]
    
    output = []
    for u in utterances:
        if not isinstance(u, dict): continue
        speaker = u.get("speaker") or "Unknown"
        
        # Determine text: use translation if language requested and available
        text = None
        if target_language:
            translations = u.get("translated_texts")
            if isinstance(translations, dict):
                text = translations.get(target_language)
        
        if not text:
            text = u.get("text") or ""
            
        # convert 0-1 scale to 0-100 scale for SDNA
        conf = float(u.get("confidence") or 1.0) * 100 
        
        val = f"Speaker {speaker}: {text}"
        output.append({
            "value": val,
            "occurrences": [build_occurrence(u.get("start"), u.get("end"), conf)]
        })
    return output

def normalize_entities(data):
    if not isinstance(data, dict): return []
    entities = data.get("entities")
    if not isinstance(entities, list): return []
    
    mapped = {}
    for e in entities:
        if not isinstance(e, dict): continue
        t = e.get("entity_type") or "unknown"
        text = e.get("text") or ""
        
        # prepend the entity type to the value so it's queryable in SDNA
        val = f"[{t}] {text}"
        
        if val not in mapped: 
            mapped[val] = []
        mapped[val].append(build_occurrence(e.get("start"), e.get("end")))
        
    return [{"value": k, "occurrences": v} for k, v in mapped.items()]

def normalize_chapters(data):
    if not isinstance(data, dict): return []
    chapters = data.get("chapters")
    if not isinstance(chapters, list): return []
    
    mapped = {}
    for c in chapters:
        if not isinstance(c, dict): continue
        headline = c.get("headline") or ""
        gist = c.get("gist") or ""
        summary = c.get("summary") or ""
        
        val = f"[{gist}] {headline}"
        if summary: 
            val += f" - {summary}"
            
        if val not in mapped: 
            mapped[val] = []
            
        mapped[val].append(build_occurrence(c.get("start"), c.get("end")))
        
    return [{"value": k, "occurrences": v} for k, v in mapped.items()]

def normalize_highlights(data):
    if not isinstance(data, dict): return []
    highlights_res = data.get("auto_highlights_result")
    if not isinstance(highlights_res, dict): return []
    
    results = highlights_res.get("results")
    if not isinstance(results, list): return []
    
    mapped = {}
    for r in results:
        if not isinstance(r, dict): continue
        val = r.get("text") or ""
        # rank usually comes in decimals like 0.08, map it up
        rank = float(r.get("rank") or 0) * 100 
        t_list = r.get("timestamps")
        if not isinstance(t_list, list): continue
        
        if val not in mapped: 
            mapped[val] = []
            
        for t in t_list:
            if not isinstance(t, dict): continue
            mapped[val].append(build_occurrence(t.get("start"), t.get("end"), rank))
            
    return [{"value": k, "occurrences": v} for k, v in mapped.items()]

def normalize_iab_categories(data):
    if not isinstance(data, dict): return []
    iab_res = data.get("iab_categories_result")
    if not isinstance(iab_res, dict): return []
    
    results = iab_res.get("results")
    if not isinstance(results, list): return []
    
    mapped = {}
    for r in results:
        if not isinstance(r, dict): continue
        t = r.get("timestamp") or {}
        labels = r.get("labels")
        if not isinstance(labels, list): continue
        
        for l in labels:
            if not isinstance(l, dict): continue
            val = l.get("label") or ""
            rel = float(l.get("relevance") or 0) * 100
            
            if val not in mapped: 
                mapped[val] = []
            # Use t.get safely
            start = t.get("start") if isinstance(t, dict) else None
            end = t.get("end") if isinstance(t, dict) else None
            mapped[val].append(build_occurrence(start, end, rel))
            
    return [{"value": k, "occurrences": v} for k, v in mapped.items()]

def normalize_content_safety(data):
    if not isinstance(data, dict): return []
    safety_res = data.get("content_safety_labels")
    if not isinstance(safety_res, dict): return []
    
    results = safety_res.get("results")
    if not isinstance(results, list): return []
    
    mapped = {}
    for r in results:
        if not isinstance(r, dict): continue
        t = r.get("timestamp") or {}
        labels = r.get("labels")
        if not isinstance(labels, list): continue
        
        for l in labels:
            if not isinstance(l, dict): continue
            sev = l.get("severity") or 0
            val = f"[{sev}] {l.get('label', '')}" 
            conf = float(l.get("confidence") or 0) * 100
            
            if val not in mapped: 
                mapped[val] = []
            
            start = t.get("start") if isinstance(t, dict) else None
            end = t.get("end") if isinstance(t, dict) else None
            mapped[val].append(build_occurrence(start, end, conf))
            
    return [{"value": k, "occurrences": v} for k, v in mapped.items()]

def normalize_summary(data):
    if not isinstance(data, dict): return []
    summ = data.get("summary") or ""
    if not summ: 
        return []
    duration = float(data.get("audio_duration") or 0) * 1000
    
    return [{
        "value": summ,
        "occurrences": [build_occurrence(0, duration)]
    }]

def normalize(assembly_json, language=None):
    """
    Scaffolds out individual mapped groups back into the master output node 
    expected by the SDNA catalog ingestion queue.
    """
    out = {}
    
    try:
        transcript = normalize_transcript(assembly_json, target_language=language)
        if transcript: 
            out["assembly_transcript"] = sort_by_timestamp(transcript)
    except Exception as e:
        print(f"Warning: Failed to normalize transcript: {e}", file=sys.stderr)

    try:
        entities = normalize_entities(assembly_json)
        if entities: 
            out["assembly_entities"] = sort_by_timestamp(entities)
    except Exception as e:
        print(f"Warning: Failed to normalize entities: {e}", file=sys.stderr)

    try:
        chapters = normalize_chapters(assembly_json)
        if chapters: 
            out["assembly_auto_chapters"] = sort_by_timestamp(chapters)
    except Exception as e:
        print(f"Warning: Failed to normalize chapters: {e}", file=sys.stderr)

    try:
        highlights = normalize_highlights(assembly_json)
        if highlights: 
            out["assembly_auto_highlights"] = sort_by_timestamp(highlights)
    except Exception as e:
        print(f"Warning: Failed to normalize highlights: {e}", file=sys.stderr)

    try:
        iab = normalize_iab_categories(assembly_json)
        if iab: 
            out["assembly_iab_categories"] = sort_by_timestamp(iab)
    except Exception as e:
        print(f"Warning: Failed to normalize IAB: {e}", file=sys.stderr)

    try:
        safety = normalize_content_safety(assembly_json)
        if safety: 
            out["assembly_content_safety"] = sort_by_timestamp(safety)
    except Exception as e:
        print(f"Warning: Failed to normalize Safety: {e}", file=sys.stderr)

    try:
        summary = normalize_summary(assembly_json)
        if summary: 
            out["assembly_summary"] = summary
    except Exception as e:
        print(f"Warning: Failed to normalize summary: {e}", file=sys.stderr)
    
    return out

def write_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Normalize AssemblyAI JSON metadata to standardized format")
    parser.add_argument("-i", "--input", required=True, help="Input JSON file")
    parser.add_argument("-o", "--output", required=True, help="Output normalized JSON file")
    parser.add_argument("-l", "--language", help="Optional language code for translated transcripts")
    return parser.parse_args()

def main():
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)
        
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        data = load_json(input_path)
        normalized = normalize(data, language=args.language)
        write_json(normalized, output_path)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
