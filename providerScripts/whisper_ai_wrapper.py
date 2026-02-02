import os
import json
import math
import subprocess
from openai import OpenAI

class WhisperASR:
    def __init__(self, api_key, chunk_duration=300, model="whisper-1"):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.chunk_duration = chunk_duration

    # -------------------------------
    # Convert input to WAV
    # -------------------------------
    def _to_wav(self, input_path, wav_path):
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-ac", "1",
            "-ar", "16000",
            wav_path
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # -------------------------------
    # Get audio duration (seconds)
    # -------------------------------
    def _get_duration(self, wav_path):
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            wav_path
        ]
        return float(subprocess.check_output(cmd).decode().strip())

    # -------------------------------
    # Split wav into chunks
    # -------------------------------
    def _split_audio(self, wav_path, output_dir, duration):
        os.makedirs(output_dir, exist_ok=True)
        chunks = []

        num_chunks = math.ceil(duration / self.chunk_duration)

        for i in range(num_chunks):
            start = i * self.chunk_duration
            out_file = os.path.join(output_dir, f"chunk_{i}.wav")

            cmd = [
                "ffmpeg", "-y",
                "-i", wav_path,
                "-ss", str(start),
                "-t", str(self.chunk_duration),
                out_file
            ]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            chunks.append((out_file, start))

        return chunks

    # -------------------------------
    # Transcribe single chunk
    # -------------------------------
    def _process_chunk(self, wav_path, language, translate=False):
        with open(wav_path, "rb") as f:
            kwargs = {
                "file": f,
                "model": self.model,
                "response_format": "verbose_json"
            }

            if language:  # ← auto-detect if None
                kwargs["language"] = language

            if translate:
                return self.client.audio.translations.create(**kwargs)
            else:
                return self.client.audio.transcriptions.create(**kwargs)

    # -------------------------------
    # Main entry point
    # -------------------------------
    def transcribe(self, input_path, output_json, language=None, translate=False):
        base = os.path.splitext(input_path)[0]
        wav_path = base + "_full.wav"
        chunk_dir = base + "_chunks"

        self._to_wav(input_path, wav_path)
        duration = self._get_duration(wav_path)

        final_segments = []
        full_text = []
        detected_language = language

        if duration <= self.chunk_duration:
            result = self._process_chunk(wav_path, language, translate)

            detected_language = "en" if translate else result.language
            full_text.append(result.text)

            for seg in result.segments:
                final_segments.append({
                    "start": float(seg.start),
                    "end": float(seg.end),
                    "text": seg.text
                })

        else:
            chunks = self._split_audio(wav_path, chunk_dir, duration)

            for chunk_path, offset in chunks:
                result = self._process_chunk(chunk_path, language, translate)

                detected_language = "en" if translate else result.language
                full_text.append(result.text)

                for seg in result.segments:
                    final_segments.append({
                        "start": float(seg.start + offset),
                        "end": float(seg.end + offset),
                        "text": seg.text
                    })

                os.remove(chunk_path)

            os.rmdir(chunk_dir)

        os.remove(wav_path)

        final_output = {
            "task": "translation" if translate else "transcription",
            "language": detected_language,
            "text": " ".join(full_text),
            "segments": final_segments
        }

        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(final_output, f, indent=2, ensure_ascii=False)

        return final_output


asr = WhisperASR(
    api_key="sk-proj******************************************************************************************************vNOLlXdnRZWkKIJfgA",
    chunk_duration=300
)

result = asr.transcribe(
    input_path=r"D:\SDNA\face_detection\ajab_prem_ki_ghazab_kahani_chunk_2.wav",
    output_json="english_1.json",
    language=None,
    translate=False
)

print("Saved JSON with", len(result["segments"]), "segments")
