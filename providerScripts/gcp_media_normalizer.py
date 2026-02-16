#!/usr/bin/env python3
"""
GCP Media Metadata Normalizer - Complete Version
Combines Google Vision AI (images) and Google Video Intelligence normalizers
Automatically detects input type and applies appropriate normalization
"""

import json
import argparse
import sys
from pathlib import Path

# ============================================================================
# AUTO-DETECTION LOGIC
# ============================================================================

def detect_input_type(json_data):
    """
    Detect whether input is Vision AI (image) or Video Intelligence (video)
    
    Vision AI indicators:
    - Has "scenes" with "frames" containing "analysis" with vision annotations
    - Direct vision annotations: label_annotations, text_annotations, etc.
    
    Video Intelligence indicators:
    - Has "annotation_results" array
    - Direct video annotations: segment_label_annotations, shot_label_annotations, etc.
    
    Returns: 'vision', 'video', or 'unknown'
    """
    if not isinstance(json_data, dict):
        return 'unknown'
    
    # Check for scenes structure (Vision AI frame-by-frame)
    if "scenes" in json_data:
        scenes = json_data.get("scenes", [])
        if scenes and isinstance(scenes, list) and len(scenes) > 0:
            first_scene = scenes[0]
            if "frames" in first_scene:
                return 'vision'
    
    # Check for direct Vision API response
    vision_keys = {
        "label_annotations", "text_annotations", "face_annotations",
        "logo_annotations", "safe_search_annotation", "image_properties_annotation",
        "localized_object_annotations", "web_detection"
    }
    if any(key in json_data for key in vision_keys):
        return 'vision'
    
    # Check for Video Intelligence wrapped response
    if "analysis" in json_data:
        analysis = json_data.get("analysis", {})
        if "annotation_results" in analysis:
            return 'video'
    
    # Check for direct Video Intelligence response
    if "annotation_results" in json_data:
        return 'video'
    
    # Check for direct video annotations
    video_keys = {
        "segment_label_annotations", "shot_label_annotations",
        "explicit_annotation", "speech_transcriptions", 
        "person_detection_annotations", "object_annotations",
        "text_annotations", "face_detection_annotations"
    }
    if any(key in json_data for key in video_keys):
        return 'video'
    
    return 'unknown'

# ============================================================================
# SHARED HELPER FUNCTIONS
# ============================================================================

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sort_by_timestamp(data_list):
    """Sort and round timestamps"""
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

def dedupe_occurrences(occurrences):
    """Remove duplicate occurrences, keeping highest confidence"""
    deduped = {}

    for occ in occurrences:
        key = (occ["start"], occ["end"])

        if key not in deduped:
            deduped[key] = occ
        else:
            # keep higher confidence
            if occ.get("confidence_score", 0) > deduped[key].get("confidence_score", 0):
                deduped[key] = occ

    return list(deduped.values())

# ============================================================================
# VISION AI HELPER FUNCTIONS
# ============================================================================

def time_to_seconds_vision(time):
    """Time conversion for Vision (HH:MM:SS format or HH-MM-SS)"""
    if not time:
        return 0
    time = time.replace("-", ":")  
    parts = time.split(":")
    if len(parts) == 3:
        hour = int(parts[0])
        minutes = int(parts[1])
        sec = float(parts[2])
        total = hour * 3600 + minutes * 60 + sec
        return round(total, 2)
    return 0

def polygon_to_bbox_vision(start, end, bounding_polygon=None, emotions=None, confidence="", catagories=None):
    """Convert polygon to bounding box for Vision AI"""
    bounding_polygon = bounding_polygon or {}
    emotions = emotions or []   
    catagories = catagories or []

    base = {
        "confidence_score": confidence,
        "start": start,
        "end": end,
        "frame": "",
        "positions": []
    }

    if emotions:
        base["emotions"] = emotions
    
    if catagories:
        base["catagories"] = catagories
    
    if not bounding_polygon:
        return base

    xs = [p.get("x", 0) for p in bounding_polygon]
    ys = [p.get("y", 0) for p in bounding_polygon]

    x1 = min(xs)
    y1 = min(ys)
    x2 = max(xs)
    y2 = max(ys)

    return {
        "confidence_score": confidence,
        "start": start,
        "end": end,
        "frame": "",
        "positions": [{"x1": x1, "y1": y1, "x2": x2, "y2": y2}],
        **({"emotions": emotions} if emotions else {}),
        **({"catagories": catagories} if catagories else {})
    }

# ============================================================================
# VISION AI NORMALIZATION FUNCTIONS
# ============================================================================

def normalize_vision_labels(json_data):
    scenes = json_data.get("scenes", [])
    label_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            label_annotations = frame.get("analysis", {}).get("label_annotations", [])

            for label in label_annotations:
                value = label.get("description", "")
                confidence = label.get("score", 0)

                if value not in label_map:
                    label_map[value] = []

                label_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        bounding_polygon={},
                        emotions=[],
                        confidence=confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in label_map.items()]

def normalize_vision_text_annotations(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            text_annotations = frame.get("analysis", {}).get("text_annotations", [])

            for text in text_annotations:
                value = text.get("description", "")
                confidence = text.get("score", 0)
                bounding_box = text.get("bounding_poly", {}).get("vertices", [])
                
                if value not in keyword_map:
                    keyword_map[value] = []
                                
                keyword_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        bounding_box,      
                        confidence=confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

def normalize_vision_object_annotations(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            object_annotations = frame.get("analysis", {}).get("localized_object_annotations", [])
            
            for obj in object_annotations:
                value = obj.get("name", "")
                confidence = obj.get("score", 0)
                bounding_box = obj.get("bounding_poly", {}).get("normalized_vertices", [])
                
                if value not in keyword_map:
                    keyword_map[value] = []
                    
                keyword_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        bounding_box,      
                        confidence=confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

def normalize_vision_face_annotations(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            face_annotations = frame.get("analysis", {}).get("face_annotations", [])
            
            for face in face_annotations:
                value = ""
                confidence = face.get("detection_confidence", 0)
                bounding_box = None
                emotions_type = ['joy_likelihood', 'sorrow_likelihood', 'anger_likelihood', 
                                'surprise_likelihood','under_exposed_likelihood','blurred_likelihood','headwear_likelihood']
                emotions = []
                for emotion in emotions_type:
                    emotions.append({
                        "type": emotion,
                        "confidence_score": "",
                        "value": face.get(emotion, "")
                    })
                
                if value not in keyword_map:
                    keyword_map[value] = []
                    
                keyword_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        bounding_box,  
                        emotions,
                        confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

def normalize_vision_safe_search_annotation(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            safe_search_annotations = frame.get("analysis", {}).get("safe_search_annotation", {})
            value = ""
            bounding_box = None
            emotion = []
            confidence = ""
            catagorie_type = ['adult', 'spoof', 'medical', 'violence','racy']
            catagories = []
            for catagorie in catagorie_type:
                catagories.append({
                    "type": catagorie,
                    "confidence_score": "",
                    "value": safe_search_annotations.get(catagorie, "")
                })
                
            if value not in keyword_map:
                keyword_map[value] = []
                
            keyword_map[value].append(
                polygon_to_bbox_vision(
                    ts_seconds, ts_seconds,
                    bounding_box,   
                    emotion,
                    confidence,   
                    catagories
                )
            )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

def normalize_vision_image_properties_annotation(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}

    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            image_properties_annotation = frame.get("analysis", {}).get("image_properties_annotation", {})
            dominant_colors = image_properties_annotation.get("dominant_colors", {}).get("colors", [])
            
            for color_info in dominant_colors:
                color = color_info.get("color", {})
                red = color.get("red", 0)
                green = color.get("green", 0)
                blue = color.get("blue", 0)
                value = f"rgb({red},{green},{blue})"
                confidence = color_info.get("score", 0)
                bounding_box = None
                emotions = []
                
                if value not in keyword_map:
                    keyword_map[value] = []
                    
                keyword_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        bounding_box,  
                        emotions,
                        confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

def normalize_vision_web_detection(json_data):
    scenes = json_data.get("scenes", [])
    keyword_map = {}
        
    for scene in scenes:
        for frame in scene.get("frames", []):
            ts_seconds = time_to_seconds_vision(frame.get("timestamp", ""))
            web_detection = frame.get("analysis", {}).get("web_detection", {})
            
            all_images = (
                web_detection.get("partial_matching_images", []) +
                web_detection.get("pages_with_matching_images", []) +
                web_detection.get("visually_similar_images", [])
            )
            
            for img in all_images:
                value = img.get("url", "")
                confidence = img.get("score", 0)
                
                if value not in keyword_map:
                    keyword_map[value] = []
                    
                keyword_map[value].append(
                    polygon_to_bbox_vision(
                        ts_seconds, ts_seconds,
                        None,  
                        [],
                        confidence*100
                    )
                )

    return [{"value": value, "occurrences": occurrences} for value, occurrences in keyword_map.items()]

# ============================================================================
# VIDEO INTELLIGENCE HELPER FUNCTIONS
# ============================================================================

def time_to_seconds_video(t):
    """Time conversion for Video Intelligence (supports '273.740134s' format)"""
    if not t:
        return 0.0

    t = t.strip()

    # Case 1: GCP format → "273.740134s"
    if t.endswith("s"):
        try:
            return round(float(t[:-1]), 2)
        except ValueError:
            return 0.0

    # Case 2: HH:MM:SS or HH:MM:SS.mmm
    if ":" in t:
        parts = t.split(":")
        if len(parts) == 3:
            h = int(parts[0])
            m = int(parts[1])
            s = float(parts[2])
            return round(h * 3600 + m * 60 + s, 2)

    return 0.0

def build_occurrence_video(confidence, start, end, adult_score=None, racy_score=None, frame="", positions=None):
    """Build occurrence for Video Intelligence"""
    base = {
        "confidence_score": confidence,
        "start": time_to_seconds_video(start),
        "end": time_to_seconds_video(end),
        "frame": frame,
        "positions": positions or []
    }
    if adult_score is not None and racy_score is not None:
        base["adult_score"] = adult_score
        base["racy_score"] = racy_score
        
    return base

def aggregate_attributes(frames):
    """Aggregate attributes from video frames"""
    attr_map = {}

    for frame in frames or []:
        for attr in frame.get("attributes", []):
            attr_type = attr.get("name", "")
            value = attr.get("value", "")
            confidence = float(attr.get("confidence", 0))
            key = (attr_type, value)

            if key not in attr_map or confidence > attr_map[key]:
                attr_map[key] = confidence

    return [
        {"type": attr_type, "value": value, "confidence": confidence}
        for (attr_type, value), confidence in attr_map.items()
    ]

def aggregate_landmarks(frames):
    """Aggregate landmarks from video frames"""
    lm_map = {}

    for frame in frames:
        for lm in frame.get("landmarks", []):
            name = lm.get("name")
            conf = lm.get("confidence", 0)

            if name not in lm_map or conf > lm_map[name]:
                lm_map[name] = conf

    return [{"type": k, "confidence": v} for k, v in lm_map.items()]

# ============================================================================
# VIDEO INTELLIGENCE NORMALIZATION FUNCTIONS
# ============================================================================

def normalize_video_segment_labels(ai_json):
    segment_labels = {}

    for data in ai_json:
        segment_label_annotations = data.get("segment_label_annotations", [])

        for ann in segment_label_annotations:
            entity = ann.get("entity", {})
            name = entity.get("description", "")

            segments = ann.get("segments", [])
            category_entities = ann.get("category_entities", [])

            for segment in segments:
                seg_time = segment.get("segment", {})
                start_time = seg_time.get("start_time_offset", "")
                end_time = seg_time.get("end_time_offset", "")
                confidence = segment.get("confidence", 0)

                occurrence = build_occurrence_video(confidence, start_time, end_time)

                # main label
                segment_labels.setdefault(name, []).append(occurrence)

                # category labels
                for cat in category_entities:
                    cat_name = cat.get("description", "")
                    segment_labels.setdefault(cat_name, []).append(occurrence.copy())

    output = []
    for label, occurrences in segment_labels.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": label, "occurrences": deduped})

    return output

def normalize_video_shot_labels(ai_json):
    shot_labels = {}

    for data in ai_json:
        shot_label_annotations = data.get("shot_label_annotations", [])

        for ann in shot_label_annotations:
            entity = ann.get("entity", {})
            name = entity.get("description", "")

            segments = ann.get("segments", [])
            category_entities = ann.get("category_entities", [])

            for segment in segments:
                seg_time = segment.get("segment", {})
                start_time = seg_time.get("start_time_offset", "")
                end_time = seg_time.get("end_time_offset", "")
                confidence = segment.get("confidence", 0)

                occurrence = build_occurrence_video(confidence, start_time, end_time)

                shot_labels.setdefault(name, []).append(occurrence)

                for cat in category_entities:
                    cat_name = cat.get("description", "")
                    shot_labels.setdefault(cat_name, []).append(occurrence.copy())

    output = []
    for label, occurrences in shot_labels.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": label, "occurrences": deduped})

    return output

def normalize_video_shot_annotations(ai_json):
    shot_annotations = []

    for data in ai_json:
        for shot in data.get("shot_annotations", []):
            seg = shot.get("segment", {})
            start_time = seg.get("start_time_offset", "")
            end_time = seg.get("end_time_offset", "")

            shot_annotations.append({
                "value": "",
                "occurrences": [build_occurrence_video(0, start_time, end_time)]
            })

    return shot_annotations

def normalize_video_explicit_annotations(ai_json):
    explicit_map = {}

    for data in ai_json:
        explicit_annotation = data.get("explicit_annotation", {})
        for frame in explicit_annotation.get("frames", []):
            time_offset = frame.get("time_offset", "")
            pornography_likelihood = frame.get("pornography_likelihood", "UNKNOWN")

            ts = time_to_seconds_video(time_offset)
            
            if pornography_likelihood not in explicit_map:
                explicit_map[pornography_likelihood] = []

            explicit_map[pornography_likelihood].append(
                build_occurrence_video(0, time_offset, time_offset)
            )

    output = []
    for value, occurrences in explicit_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": value, "occurrences": deduped})

    return output

def normalize_video_segment(ai_json):
    segments = []

    for data in ai_json:
        for shot in data.get("shot_annotations", []):
            seg = shot.get("segment", {})
            start_time = seg.get("start_time_offset", "")
            end_time = seg.get("end_time_offset", "")

            segments.append({
                "start": time_to_seconds_video(start_time),
                "end": time_to_seconds_video(end_time)
            })

    return segments

def normalize_video_text_annotations(ai_json):
    text_map = {}

    for data in ai_json:
        for text_annotation in data.get("text_annotations", []):
            text = text_annotation.get("text", "")
            
            for segment in text_annotation.get("segments", []):
                seg_time = segment.get("segment", {})
                start_time = seg_time.get("start_time_offset", "")
                end_time = seg_time.get("end_time_offset", "")
                confidence = segment.get("confidence", 0)

                frames = segment.get("frames", [])
                positions = []
                for frame in frames:
                    bbox = frame.get("rotated_bounding_box", {})
                    vertices = bbox.get("vertices", [])
                    if vertices and len(vertices) >= 4:
                        xs = [v.get("x", 0) for v in vertices]
                        ys = [v.get("y", 0) for v in vertices]
                        positions.append({
                            "x1": min(xs), "y1": min(ys),
                            "x2": max(xs), "y2": max(ys)
                        })

                text_map.setdefault(text, []).append(
                    build_occurrence_video(confidence, start_time, end_time, positions=positions)
                )

    output = []
    for text, occurrences in text_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": text, "occurrences": deduped})

    return output

def normalize_video_face_detection_annotations(ai_json):
    face_map = {}

    for data in ai_json:
        for face_detection_annotation in data.get("face_detection_annotations", []):
            for track in face_detection_annotation.get("tracks", []):
                segment = track.get("segment", {})
                start_time = segment.get("start_time_offset", "")
                end_time = segment.get("end_time_offset", "")

                frames = track.get("timestamped_objects", [])
                positions = []

                for frame in frames:
                    bbox = frame.get("normalized_bounding_box")
                    if bbox:
                        positions.append({
                            "x1": bbox.get("left"), "y1": bbox.get("top"),
                            "x2": bbox.get("right"), "y2": bbox.get("bottom")
                        })

                attributes = aggregate_attributes(frames)
                
                occurrence = build_occurrence_video(0, start_time, end_time, positions=positions)
                occurrence["attributes"] = attributes

                face_map.setdefault("", []).append(occurrence)

    output = []
    for value, occurrences in face_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": value, "occurrences": deduped})

    return output

def normalize_video_object_annotations(ai_json):
    object_map = {}

    for data in ai_json:
        for object_annotation in data.get("object_annotations", []):
            entity = object_annotation.get("entity", {})
            label = entity.get("description", "")

            for track in object_annotation.get("tracks", []):
                segment = track.get("segment", {})
                start_time = segment.get("start_time_offset", "")
                end_time = segment.get("end_time_offset", "")
                confidence = track.get("confidence", 0)

                frames = track.get("timestamped_objects", [])
                positions = []

                for frame in frames:
                    bbox = frame.get("normalized_bounding_box")
                    if bbox:
                        positions.append({
                            "x1": bbox.get("left"), "y1": bbox.get("top"),
                            "x2": bbox.get("right"), "y2": bbox.get("bottom")
                        })

                object_map.setdefault(label, []).append(
                    build_occurrence_video(confidence, start_time, end_time, positions=positions)
                )

    output = []
    for label, occurrences in object_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": label, "occurrences": deduped})

    return output

def normalize_video_logo_recognition_annotations(ai_json):
    logo_annotations = {}

    for data in ai_json:
        for logo_annotation in data.get("logo_recognition_annotations", []):
            entity = logo_annotation.get("entity", {})
            label = entity.get("description", "")

            for track in logo_annotation.get("tracks", []):
                segment = track.get("segment", {})
                start_time = segment.get("start_time_offset", "")
                end_time = segment.get("end_time_offset", "")
                confidence = track.get("confidence", 0)

                frames = track.get("timestamped_objects", [])
                positions = []

                for frame in frames:
                    bbox = frame.get("normalized_bounding_box")
                    if bbox:
                        positions.append({
                            "x1": bbox.get("left"), "y1": bbox.get("top"),
                            "x2": bbox.get("right"), "y2": bbox.get("bottom")
                        })

                logo_annotations.setdefault(label, []).append(
                    build_occurrence_video(confidence, start_time, end_time, positions=positions)
                )

    output = []
    for label, occurrences in logo_annotations.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": label, "occurrences": deduped})

    return output

def normalize_video_speech_transcripts(ai_json):
    transcripts = {}
    
    for data in ai_json:
        for item in data.get("speech_transcriptions", []):
            for alt in item.get("alternatives", []):
                text = alt.get("transcript", "").strip()
                confidence = alt.get("confidence", 0)

                words = alt.get("words", [])
                if not words:
                    continue

                start_time = words[0].get("start_time", "")
                end_time = words[-1].get("end_time", "")

                transcripts.setdefault(text, []).append(
                    build_occurrence_video(confidence, start_time, end_time)
                )

    output = []
    for text, occurrences in transcripts.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": text, "occurrences": deduped})

    return output

def normalize_video_speech_words(ai_json):
    words_map = {}
    
    for data in ai_json:
        for item in data.get("speech_transcriptions", []):
            for alt in item.get("alternatives", []):
                for word_info in alt.get("words", []):
                    word = word_info.get("word", "").strip()
                    if not word:
                        continue

                    start_time = word_info.get("start_time", "")
                    end_time = word_info.get("end_time", "")
                    confidence = word_info.get("confidence", 0)

                    words_map.setdefault(word, []).append(
                        build_occurrence_video(confidence, start_time, end_time)
                    )

    output = []
    for word, occurrences in words_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": word, "occurrences": deduped})

    return output

def normalize_video_person_detection_annotations(ai_json):
    tracks_map = {}
    
    for data in ai_json:
        for person_detection_annotation in data.get("person_detection_annotations", []):
            for track in person_detection_annotation.get("tracks", []):
                segment = track.get("segment", {})
                start_time = segment.get("start_time_offset", "")
                end_time = segment.get("end_time_offset", "")
                confidence = track.get("confidence", 0)
                frames = track.get("timestamped_objects", [])
                positions = []

                for frame in frames:
                    bbox = frame.get("normalized_bounding_box")
                    if bbox:
                        positions.append({
                            "x1": bbox.get("left"), "y1": bbox.get("top"),
                            "x2": bbox.get("right"), "y2": bbox.get("bottom")
                        })

                attributes = aggregate_attributes(frames)
                landmarks = aggregate_landmarks(frames)
                
                occurrence = build_occurrence_video(confidence, start_time, end_time, positions=positions)
                occurrence["attributes"] = attributes
                occurrence["landmarks"] = landmarks

                tracks_map.setdefault("", []).append(occurrence)

    output = []
    for value, occurrences in tracks_map.items():
        deduped = dedupe_occurrences(occurrences)
        deduped.sort(key=lambda x: x["start"])
        output.append({"value": value, "occurrences": deduped})

    return output

# ============================================================================
# MAIN NORMALIZATION DISPATCHERS
# ============================================================================

def normalize_vision(json_data):
    """Main normalization for Vision AI data"""
    print("  → Processing Vision AI normalization...")
    
    labels = normalize_vision_labels(json_data)
    text_annotations = normalize_vision_text_annotations(json_data)
    object_annotations = normalize_vision_object_annotations(json_data)
    face_annotations = normalize_vision_face_annotations(json_data)
    safe_search_annotation = normalize_vision_safe_search_annotation(json_data)
    image_properties_annotation = normalize_vision_image_properties_annotation(json_data)
    web_detection = normalize_vision_web_detection(json_data)
    
    return {
        "gcp_vision_label_annotations": sort_by_timestamp(labels),
        "gcp_vision_object_annotations": sort_by_timestamp(object_annotations),
        "gcp_vision_text_annotations": sort_by_timestamp(text_annotations),
        "gcp_vision_face_annotations": sort_by_timestamp(face_annotations),
        "gcp_vision_safe_search_annotation": sort_by_timestamp(safe_search_annotation),
        "gcp_vision_image_properties_annotation": sort_by_timestamp(image_properties_annotation),
        "gcp_vision_web_detection": sort_by_timestamp(web_detection),
    }

def normalize_video(ai_json):
    """Main normalization for Video Intelligence data"""
    print("  → Processing Video Intelligence normalization...")
    
    # Extract annotation_results if wrapped
    if isinstance(ai_json, dict):
        if "analysis" in ai_json and "annotation_results" in ai_json["analysis"]:
            annotation_results = ai_json["analysis"]["annotation_results"]
        elif "annotation_results" in ai_json:
            annotation_results = ai_json["annotation_results"]
        else:
            annotation_results = [ai_json]
    else:
        annotation_results = ai_json if isinstance(ai_json, list) else [ai_json]
    
    segment_labels = normalize_video_segment_labels(annotation_results)
    shot_labels = normalize_video_shot_labels(annotation_results)
    shot_annotations = normalize_video_shot_annotations(annotation_results)
    explicit_annotations = normalize_video_explicit_annotations(annotation_results)
    segment = normalize_video_segment(annotation_results)
    text_annotations = normalize_video_text_annotations(annotation_results)
    face_detection_annotations = normalize_video_face_detection_annotations(annotation_results)
    object_annotations = normalize_video_object_annotations(annotation_results)
    logo_recognition_annotations = normalize_video_logo_recognition_annotations(annotation_results)
    speech_transcripts = normalize_video_speech_transcripts(annotation_results)
    speech_words = normalize_video_speech_words(annotation_results)
    person_detection_annotations = normalize_video_person_detection_annotations(annotation_results)
    
    return {
        "gcp_video_segment_label_annotations": sort_by_timestamp(segment_labels),
        "gcp_video_shot_label_annotations": sort_by_timestamp(shot_labels),
        "gcp_video_shot_annotations": sort_by_timestamp(shot_annotations),
        "gcp_video_explicit_annotations": sort_by_timestamp(explicit_annotations),
        "gcp_video_segment": segment,
        "gcp_video_text_annotations": sort_by_timestamp(text_annotations),
        "gcp_video_face_detection_annotations": sort_by_timestamp(face_detection_annotations),
        "gcp_video_object_annotations": sort_by_timestamp(object_annotations),
        "gcp_video_logo_recognition_annotations": sort_by_timestamp(logo_recognition_annotations),
        "gcp_video_speech_transcripts": sort_by_timestamp(speech_transcripts),
        "gcp_video_speech_words": sort_by_timestamp(speech_words),
        "gcp_video_person_detection_annotations": sort_by_timestamp(person_detection_annotations),
    }

def normalize(json_data):
    """
    Main entry point - detects type and applies appropriate normalization
    """
    input_type = detect_input_type(json_data)
    
    print(f"Detected input type: {input_type.upper()}")
    
    if input_type == 'vision':
        return normalize_vision(json_data)
    
    elif input_type == 'video':
        return normalize_video(json_data)
    
    else:
        print("Warning: Could not auto-detect input type.")
        print("Attempting Video Intelligence normalization as fallback...")
        try:
            return normalize_video(json_data)
        except Exception as e:
            print(f"Video normalization failed: {e}")
            print("Attempting Vision AI normalization as fallback...")
            return normalize_vision(json_data)

# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Normalize GCP Media (Vision AI or Video Intelligence) JSON metadata"
    )
    parser.add_argument(
        "input",
        help="Path to input JSON file (raw metadata from GCP)"
    )
    parser.add_argument(
        "output",
        help="Path to output normalized JSON file"
    )
    parser.add_argument(
        "-t", "--type",
        choices=['vision', 'video', 'auto'],
        default='auto',
        help="Force input type (default: auto-detect)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
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
        if args.verbose:
            print(f"\n{'='*60}")
            print(f"GCP Media Metadata Normalizer")
            print(f"{'='*60}")
        
        print(f"Loading input file: {input_path}")
        json_data = load_json(input_path)
        
        print("Normalizing data...")
        normalized = normalize(json_data)
        
        print(f"Writing output to: {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=4)
        
        if args.verbose:
            print(f"\n{'='*60}")
        print("✓ Normalization complete!")
        if args.verbose:
            print(f"{'='*60}\n")
        
        sys.exit(0)
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()