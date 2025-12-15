import json
import argparse
import sys
from pathlib import Path

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
    
def time_to_seconds(t):
    if not t or t == "":
        return 0

    parts = t.split(":")
    h = int(parts[0])
    m = int(parts[1])
    s = float(parts[2])
    total = h * 3600 + m * 60 + s

    return round(total, 2)

def build_occurrence(confidence, start, end, frame="", positions=None):
    return {
        "confidence_score": confidence,
        "start": time_to_seconds(start),
        "end": time_to_seconds(end),
        "frame": frame,
        "positions": positions or []
    }

def sort_by_timestamp(data_list):
    return sorted(
        data_list,
        key=lambda x: x["occurrences"][0]["start"] if x.get("occurrences") else float('inf')
    )

def normalize_transcript(ai_json):
    transcript = ai_json.get("transcript", [])
    output = []
    for t in transcript:
        text = t.get("text", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(t.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    return output

def normalize_ocr(ai_json):
    ocr = ai_json.get("ocr", [])
    output = []

    for o in ocr:
        text = o.get("text", "")
        occs = []

        for inst in o.get("instances", []):
            occ = build_occurrence(
                confidence=int(o.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end"),
                positions=[{
                    "x1": o.get("left", ""),
                    "x2": o.get("left", "") + o.get("width", 0),
                    "y1": o.get("top", ""),
                    "y2": o.get("top", "") + o.get("height", 0)
                }]
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    return output

def normalize_labels(ai_json):
    labels = ai_json.get("labels", [])
    output = []
    for label in labels:
        name = label.get("name", "")
        occurrences = []
        for inst in label.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occurrences.append(occ)

        output.append({
            "value": name,
            "occurrences": occurrences
        })
    return output

def normalize_combined_keywords(ai_json):
    output = []
    keywords = ai_json.get("keywords", [])
    for k in keywords:
        text = k.get("text", "")
        occurrences = []
        for inst in k.get("instances", []):
            occ = build_occurrence(
                confidence=int(k.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occurrences.append(occ)

        output.append({
            "value": text,
            "occurrences": occurrences
        })

    topics = ai_json.get("topics", [])
    for k in topics:
        text = k.get("name", "")
        occurrences = []
        for inst in k.get("instances", []):
            occ = build_occurrence(
                confidence=int(k.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occurrences.append(occ)

        output.append({
            "value": text,
            "occurrences": occurrences
        })

    brands = ai_json.get("brands", [])
    for k in brands:
        text = k.get("name", "")
        occurrences = []
        for inst in k.get("instances", []):
            occ = build_occurrence(
                confidence=int(k.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occurrences.append(occ)

        output.append({
            "value": text,
            "occurrences": occurrences
        })
    
    namedLocations = ai_json.get("namedLocations", [])
    for k in namedLocations:
        text = k.get("name", "")
        occurrences = []
        for inst in k.get("instances", []):
            occ = build_occurrence(
                confidence=int(k.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occurrences.append(occ)

        output.append({
            "value": text,
            "occurrences": occurrences
        })

    return output

def normalize_persons(ai_json):
    people = ai_json.get("namedPeople", [])
    output = []

    for p in people:
        name = p.get("name", "")
        occs = []

        for inst in p.get("instances", []):
            occ = build_occurrence(
                confidence=int(p.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": name,
            "occurrences": occs
        })

    return output

def normalize_combined_scenes(ai_json):
    final_list = []
    scenes = ai_json.get("scenes", [])
    for sh in scenes:
        occs = []
        for inst in sh.get("instances", []):
            occ = build_occurrence(
                confidence="",
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)
        final_list.append({
            "value": "", 
            "occurrences": occs
        })

    shots = ai_json.get("shots", [])
    for sh in shots:
        occs = []
        for inst in sh.get("instances", []):
            occ = build_occurrence(
                confidence="",
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)
        final_list.append({
            "value": "", 
            "occurrences": occs
        })
    
    blocks = ai_json.get("blocks", [])
    for sh in blocks:
        occs = []
        for inst in sh.get("instances", []):
            occ = build_occurrence(
                confidence="",
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)
        final_list.append({
            "value": "", 
            "occurrences": occs
        })

    return final_list

def normalize_obj(ai_json):
    detectedObjects = ai_json.get("detectedObjects", [])
    output = []
    for t in detectedObjects:
        text = t.get("type", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    return output

def normalize_speeches(ai_json):
    speakers = ai_json.get("speakers", [])
    output = []
    for t in speakers:
        text = t.get("name", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence="",
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    return output

def normalize_emotions(ai_json):
    output = []
    emotions = ai_json.get("emotions", [])
    for t in emotions:
        text = t.get("type", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    sentiments = ai_json.get("sentiments", [])
    for t in sentiments:
        text = t.get("sentimentType", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(t.get("averageScore", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })

    audioEffects = ai_json.get("audioEffects", [])
    for t in audioEffects:
        text = t.get("type", "")
        occs = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            occs.append(occ)

        output.append({
            "value": text,
            "occurrences": occs
        })
    return output


def normalize(ai_json):
    persons = normalize_persons(ai_json)
    labels = normalize_labels(ai_json)
    speeches = normalize_speeches(ai_json)
    ocr = normalize_ocr(ai_json)
    transcript = normalize_transcript(ai_json)
    keywords = normalize_combined_keywords(ai_json)
    objects_ = normalize_obj(ai_json)
    emotions = normalize_emotions(ai_json)
    scenes = normalize_combined_scenes(ai_json)

    return {
        "scenes": sort_by_timestamp(scenes),
        "summary": [],
        "persons": sort_by_timestamp(persons),
        "labels": sort_by_timestamp(labels),
        "speeches": sort_by_timestamp(speeches),
        "ocr": sort_by_timestamp(ocr),
        "transcript": sort_by_timestamp(transcript),
        "keywords": sort_by_timestamp(keywords),
        "objects": sort_by_timestamp(objects_),
        "emotions": sort_by_timestamp(emotions),
        "embedding": []
    }

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Normalize Azure Video AI JSON metadata to standardized format"
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Path to input JSON file"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path to output normalized JSON file"
    )
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    # Validate input file exists
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        print(f"\nLoading input file: {input_path}")
        ai_json = load_json(input_path)
        
        print("Normalizing data...")
        normalized = normalize(ai_json)
        
        print(f"Writing output to: {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=4)
        
        print("✓ Normalization complete!")
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
