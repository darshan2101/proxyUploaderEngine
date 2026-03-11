import json
import argparse
import sys
from pathlib import Path

LANGUAGE_CODE_MAP = {
  "": "default",
  "ar-AE": "Arabic (United Arab Emirates)",
  "ar-BH": "Arabic Modern Standard (Bahrain)",
  "ar-EG": "Arabic Egypt",
  "ar-IL": "Arabic (Israel)",
  "ar-IQ": "Arabic (Iraq)",
  "ar-JO": "Arabic (Jordan)",
  "ar-KW": "Arabic (Kuwait)",
  "ar-LB": "Arabic (Lebanon)",
  "ar-OM": "Arabic (Oman)",
  "ar-PS": "Arabic (Palestinian Authority)",
  "ar-QA": "Arabic (Qatar)",
  "ar-SA": "Arabic (Saudi Arabia)",
  "ar-SY": "Arabic Syrian Arab Republic",
  "bg-BG": "Bulgarian",
  "ca-ES": "Catalan",
  "cs-CZ": "Czech",
  "da-DK": "Danish",
  "de-DE": "German",
  "el-GR": "Greek",
  "en-AU": "English Australia",
  "en-GB": "English United Kingdom",
  "en-US": "English United States",
  "es-ES": "Spanish",
  "es-MX": "Spanish (Mexico)",
  "et-EE": "Estonian",
  "fa-IR": "Persian",
  "fi-FI": "Finnish",
  "fil-PH": "Filipino",
  "fr-CA": "French (Canada)",
  "fr-FR": "French",
  "ga-IE": "Irish",
  "gu-IN": "Gujarati",
  "he-IL": "Hebrew",
  "hi-IN": "Hindi",
  "hr-HR": "Croatian",
  "hu-HU": "Hungarian",
  "hy-AM": "Armenian",
  "id-ID": "Indonesian",
  "is-IS": "Icelandic",
  "it-IT": "Italian",
  "ja-JP": "Japanese",
  "kn-IN": "Kannada",
  "ko-KR": "Korean",
  "lt-LT": "Lithuanian",
  "lv-LV": "Latvian",
  "ml-IN": "Malayalam",
  "ms-MY": "Malay",
  "nb-NO": "Norwegian",
  "nl-NL": "Dutch",
  "pl-PL": "Polish",
  "pt-BR": "Portuguese",
  "pt-PT": "Portuguese (Portugal)",
  "ro-RO": "Romanian",
  "ru-RU": "Russian",
  "sk-SK": "Slovak",
  "sl-SI": "Slovenian",
  "sv-SE": "Swedish",
  "ta-IN": "Tamil",
  "te-IN": "Telugu",
  "th-TH": "Thai",
  "tr-TR": "Turkish",
  "uk-UA": "Ukrainian",
  "vi-VN": "Vietnamese",
  "zh-Hans": "Chinese (Simplified)",
  "zh-HK": "Chinese (Cantonese, Traditional)"
}


#########################
# Healper functions
#########################

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

def build_occurrence(confidence, start, end, adult_score=None, racy_score=None, frame="", positions=None):
    base = {
        "confidence_score": confidence,
        "start": time_to_seconds(start),
        "end": time_to_seconds(end),
        "frame": frame,
        "positions": positions or []
    }
    if adult_score is not None and racy_score is not None:
        base["adult_score"] = adult_score
        base["racy_score"] = racy_score
        
    return base

def sort_by_timestamp(data_list):
    data_list = sorted(
        data_list,
        key=lambda x: x["occurrences"][0]["start"] if x.get("occurrences") else float('inf')
    )

    # Round start & end to 2 decimals
    for item in data_list:
        if item.get("occurrences"):
            for occ in item["occurrences"]:
                occ["start"] = round(occ["start"], 2)
                occ["end"] = round(occ["end"], 2)

    return data_list

#########################
# Normalize functions
#########################

def normalize_transcript(ai_json):
    transcript = ai_json.get("transcript", [])
    output = []
    grouped = {}  
    
    for t in transcript:
        text = t.get("text", "")
        
        if text not in grouped:
            grouped[text] = [] 
        
        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(t.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_ocr(ai_json):
    ocr = ai_json.get("ocr", [])
    output = []
    grouped = {}

    for o in ocr:
        text = o.get("text", "")
        
        if text not in grouped:
            grouped[text] = []

        for inst in o.get("instances", []):
            occ = build_occurrence(
                confidence=int(o.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end"),
                adult_score=None,
                racy_score=None,
                positions=[{
                    "x1": o.get("left", ""),
                    "x2": o.get("left", "") + o.get("width", 0),
                    "y1": o.get("top", ""),
                    "y2": o.get("top", "") + o.get("height", 0)
                }]
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_keywords(ai_json):
    output = []
    keywords = ai_json.get("keywords", [])
    grouped = {}
    
    for k in keywords:
        text = k.get("text", "")
        
        if text not in grouped:
            grouped[text] = []
            
        for inst in k.get("instances", []):
            occ = build_occurrence(
                confidence=int(k.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
            output.append({
                "value": obj_type,
                "occurrences": occ_list
            })
        
    return output

def normalize_topics(ai_json):
    output = []
    grouped = {}
    topics = ai_json.get("topics", [])
    
    for topic in topics:
        text = topic.get("name", "")
        
        if text not in grouped:
            grouped[text] = [] 

        for inst in topic.get("instances", []):
            occ = build_occurrence(
                confidence=int(topic.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)
    
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_labels(ai_json):
    labels = ai_json.get("labels", [])
    output = []
    grouped = {}
    
    for label in labels:
        name = label.get("name", "")
        if name not in grouped:
            grouped[name] = []
            
        for inst in label.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[name].append(occ)

        
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_brands(ai_json):
    output = []
    grouped = {}     
    brands = ai_json.get("brands", [])
    
    for brand in brands:
        text = brand.get("name", "")
        
        if text not in grouped:
            grouped[text] = []

        for inst in brand.get("instances", []):
            occ = build_occurrence(
                confidence=int(brand.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)
    
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_named_locations(ai_json):
    output = []
    grouped = {}    
    namedLocations = ai_json.get("namedLocations", [])
    
    for location in namedLocations:
        text = location.get("name", "")
        
        if text not in grouped:
            grouped[text] = []

        for inst in location.get("instances", []):
            occ = build_occurrence(
                confidence=int(location.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)
    
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_named_people(ai_json):
    people = ai_json.get("namedPeople", [])
    output = []
    grouped = {}
    
    for p in people:
        name = p.get("name", "")
        
        if name not in grouped:
            grouped[name] = []

        for inst in p.get("instances", []):
            occ = build_occurrence(
                confidence=int(p.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[name].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_audio_effects(ai_json):
    output = []
    grouped = {}
    audioEffects = ai_json.get("audioEffects", [])
    
    for t in audioEffects:
        text = t.get("type", "")
        
        if text not in grouped:
            grouped[text] = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)
    
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_detected_objects(ai_json):
    detected_objects = ai_json.get("detectedObjects", [])
    
    grouped = {}  
    
    for t in detected_objects:
        text = t.get("type", "")
        
        if text not in grouped:
            grouped[text] = []  
        
        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    output = []
    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_sentiments(ai_json):
    output = []
    grouped = {}
    sentiments = ai_json.get("sentiments", [])
    
    for sentiment in sentiments:
        text = sentiment.get("sentimentType", "")
        if text not in grouped:
            grouped[text] = []

        for inst in sentiment.get("instances", []):
            occ = build_occurrence(
                confidence=int(sentiment.get("averageScore", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_emotions(ai_json):
    output = []
    grouped = {}
    emotions = ai_json.get("emotions", [])
    
    for t in emotions:
        text = t.get("type", "")
        if text not in grouped:
            grouped[text] = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(inst.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_speakers(ai_json):
    speakers = ai_json.get("speakers", [])
    output = []
    grouped = {}
    
    for t in speakers:
        text = t.get("name", "")
        if text not in grouped:
            grouped[text] = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence="",
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_frame_patterns(ai_json):
    frame_patterns = ai_json.get("framePatterns", [])
    output = []
    grouped = {}
    
    for t in frame_patterns:
        text = t.get("patternType", "")
        if text not in grouped:
            grouped[text] = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(t.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end")
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

def normalize_visual_content_moderation(ai_json):
    visual_content_moderation = ai_json.get("visualContentModeration", [])
    output = []
    grouped = {}
    
    for t in visual_content_moderation:
        text = ""
        if text not in grouped:
            grouped[text] = []

        for inst in t.get("instances", []):
            occ = build_occurrence(
                confidence=int(t.get("confidence", 0) * 100),
                start=inst.get("start"),
                end=inst.get("end"),
                adult_score=int(t.get("adultScore", 0) * 100),
                racy_score=int(t.get("racyScore", 0) * 100)
            )
            grouped[text].append(occ)

    for obj_type, occ_list in grouped.items():
        output.append({
            "value": obj_type,
            "occurrences": occ_list
        })
    
    return output

#########################
# Main functions
#########################

def normalize(ai_json):
    transcript = normalize_transcript(ai_json)
    ocr = normalize_ocr(ai_json)
    keywords = normalize_keywords(ai_json)
    topics = normalize_topics(ai_json)
    labels = normalize_labels(ai_json)
    brands = normalize_brands(ai_json)
    named_locations = normalize_named_locations(ai_json)
    named_people = normalize_named_people(ai_json)
    audio_effects = normalize_audio_effects(ai_json)
    detected_objects = normalize_detected_objects(ai_json)
    sentimets = normalize_sentiments(ai_json)
    emotions = normalize_emotions(ai_json)
    speakers = normalize_speakers(ai_json)
    frame_patterns = normalize_frame_patterns(ai_json) 
    visual_content_moderation = normalize_visual_content_moderation(ai_json)

    return {
        "azure_video_transcript": sort_by_timestamp(transcript),
        "azure_video_ocr": sort_by_timestamp(ocr),
        "azure_video_keywords": sort_by_timestamp(keywords),
        "azure_video_topics": sort_by_timestamp(topics),
        "azure_video_labels": sort_by_timestamp(labels),
        "azure_video_brands": sort_by_timestamp(brands),
        "azure_video_named_locations": sort_by_timestamp(named_locations),
        "azure_video_named_people": sort_by_timestamp(named_people),
        "azure_video_audio_effects": sort_by_timestamp(audio_effects),
        "azure_video_detected_objects": sort_by_timestamp(detected_objects),
        "azure_video_sentiments": sort_by_timestamp(sentimets),
        "azure_video_emotions": sort_by_timestamp(emotions),
        "azure_video_visual_content_moderation": sort_by_timestamp(visual_content_moderation),
        "azure_video_frame_patterns": sort_by_timestamp(frame_patterns),
        "azure_video_speakers": sort_by_timestamp(speakers),
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

def write_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

        default_data = ai_json.get("insights")
        multilingual_data = ai_json.get("multilanguage_insights", {})

        # Case 1: insights key exists
        if default_data:
            normalized = normalize(default_data)
            write_json(normalized, output_path)

            # Handle multilingual outputs
            for lang, value in multilingual_data.items():
                print(f"Normalizing multilingual data for language: {lang}")
                lang_normalized = normalize(value)

                lang_output_path = output_path.with_name(
                    f"{output_path.stem}_{lang}{output_path.suffix}"
                )
                write_json(lang_normalized, lang_output_path)

        # Case 2: no insights key → normalize full JSON
        else:
            normalized = normalize(ai_json)
            write_json(normalized, output_path)

        print("✓ Normalization complete!")

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
if __name__ == "__main__":
    main()
