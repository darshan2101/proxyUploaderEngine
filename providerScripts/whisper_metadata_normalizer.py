import json
import argparse
import sys
from pathlib import Path

WHISPER_LANGUAGES = {
    "en": "English", "zh": "Chinese", "de": "German", "es": "Spanish",
    "ru": "Russian", "ko": "Korean", "fr": "French", "ja": "Japanese",
    "pt": "Portuguese", "tr": "Turkish", "pl": "Polish", "ca": "Catalan",
    "nl": "Dutch", "ar": "Arabic", "sv": "Swedish", "it": "Italian",
    "id": "Indonesian", "hi": "Hindi", "fi": "Finnish", "vi": "Vietnamese",
    "he": "Hebrew", "uk": "Ukrainian", "el": "Greek", "ms": "Malay",
    "cs": "Czech", "ro": "Romanian", "da": "Danish", "hu": "Hungarian",
    "ta": "Tamil", "no": "Norwegian", "th": "Thai", "ur": "Urdu",
    "hr": "Croatian", "bg": "Bulgarian", "lt": "Lithuanian", "la": "Latin",
    "mi": "Maori", "ml": "Malayalam", "cy": "Welsh", "sk": "Slovak",
    "te": "Telugu", "fa": "Persian", "lv": "Latvian", "bn": "Bengali",
    "sr": "Serbian", "az": "Azerbaijani", "sl": "Slovenian", "kn": "Kannada",
    "et": "Estonian", "mk": "Macedonian", "br": "Breton", "eu": "Basque",
    "is": "Icelandic", "hy": "Armenian", "ne": "Nepali", "mn": "Mongolian",
    "bs": "Bosnian", "kk": "Kazakh", "sq": "Albanian", "sw": "Swahili",
    "gl": "Galician", "mr": "Marathi", "pa": "Punjabi", "si": "Sinhala",
    "km": "Khmer", "sn": "Shona", "yo": "Yoruba", "so": "Somali",
    "af": "Afrikaans", "oc": "Occitan", "ka": "Georgian", "be": "Belarusian",
    "tg": "Tajik", "sd": "Sindhi", "gu": "Gujarati", "am": "Amharic",
    "yi": "Yiddish", "lo": "Lao", "uz": "Uzbek", "fo": "Faroese",
    "ht": "Haitian Creole", "ps": "Pashto", "tk": "Turkmen", "nn": "Nynorsk",
    "mt": "Maltese", "sa": "Sanskrit", "lb": "Luxembourgish", "my": "Myanmar",
    "bo": "Tibetan", "tl": "Tagalog", "mg": "Malagasy", "as": "Assamese",
    "tt": "Tatar", "haw": "Hawaiian", "ln": "Lingala", "ha": "Hausa",
    "ba": "Bashkir", "jw": "Javanese", "su": "Sundanese"
}

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_occurrence(start, end):
    return {
        "confidence_score": 100,
        "start": round(start, 2),
        "end": round(end, 2)
    }

def sort_by_timestamp(data_list):
    return sorted(
        data_list,
        key=lambda x: x["occurrences"][0]["start"] if x.get("occurrences") else float('inf')
    )

def normalize_transcript(whisper_json):
    segments = whisper_json.get("segments", [])
    output = []
    grouped = {}
    
    for seg in segments:
        text = seg.get("text", "").strip()
        if not text:
            continue
        
        if text not in grouped:
            grouped[text] = []
        
        occ = build_occurrence(
            start=seg.get("start", 0),
            end=seg.get("end", 0)
        )
        grouped[text].append(occ)
    
    for value, occ_list in grouped.items():
        output.append({
            "value": value,
            "occurrences": occ_list
        })
    
    return output

def normalize_full_text(whisper_json):
    full_text = whisper_json.get("text", "")
    duration = whisper_json.get("duration", 0)
    
    if not full_text:
        return []
    
    return [{
        "value": full_text,
        "occurrences": [build_occurrence(start=0, end=duration)]
    }]

def extract_words_from_segments(whisper_json):
    segments = whisper_json.get("segments", [])
    word_grouped = {}
    
    for seg in segments:
        text = seg.get("text", "").strip()
        words = text.split()
        
        for word in words:
            word_clean = word.strip().lower()
            if len(word_clean) < 3:
                continue
            
            if word_clean not in word_grouped:
                word_grouped[word_clean] = []
            
            occ = build_occurrence(
                start=seg.get("start", 0),
                end=seg.get("end", 0)
            )
            word_grouped[word_clean].append(occ)
    
    output = []
    for value, occ_list in word_grouped.items():
        output.append({
            "value": value,
            "occurrences": occ_list
        })
    
    return output

def normalize(whisper_json):
    transcript_segments = normalize_transcript(whisper_json)
    full_text = normalize_full_text(whisper_json)
    keywords = extract_words_from_segments(whisper_json)
    
    return {
        "whisper_transcript": sort_by_timestamp(transcript_segments),
        "whisper_full_text": full_text,
        "whisper_keywords": sort_by_timestamp(keywords)
    }

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Normalize Whisper AI JSON metadata to standardized format"
    )
    parser.add_argument("-i", "--input", required=True, help="Input JSON file")
    parser.add_argument("-o", "--output", required=True, help="Output normalized JSON file")
    return parser.parse_args()

def write_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        print(f"\nLoading input file: {input_path}")
        whisper_json = load_json(input_path)
        
        print("Normalizing data...")
        
        base_data = whisper_json.get("base_transcription")
        multilingual_data = whisper_json.get("multilanguage_transcriptions", {})
        
        if base_data:
            normalized = normalize(base_data)
            write_json(normalized, output_path)
            
            for lang, lang_data in multilingual_data.items():
                print(f"Normalizing for language: {lang}")
                lang_normalized = normalize(lang_data)
                lang_output_path = output_path.with_name(
                    f"{output_path.stem}_{lang}{output_path.suffix}"
                )
                write_json(lang_normalized, lang_output_path)
        else:
            normalized = normalize(whisper_json)
            write_json(normalized, output_path)
        
        print("✓ Normalization complete!")
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
