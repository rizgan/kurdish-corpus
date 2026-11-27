"""Small, deterministic character n-gram discriminator for Kurmanji vs Zazaki.

Train on the full HF JSONL (randomized hash-ranked sample; balanced train and
held-out partitions), then call ``classify(text, model)`` from the crawler.
Labels come from high-confidence HF LID rows, never from script heuristics.
"""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import math
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

MODEL_VERSION = 1
FEATURE_BITS = 16
FEATURE_MASK = (1 << FEATURE_BITS) - 1
TRAIN_PER_CLASS = 3000
VALIDATION_PER_CLASS = 600
MIN_TRAIN = 200
MIN_VALIDATION = 50
MIN_CONFIDENT_PREDICTIONS = 20
TARGET_PRECISION = 0.98
ZAZAKI_LABELS = {"diq_Latn", "kiu_Latn"}
TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def _canonical(text: str) -> str:
    return unicodedata.normalize("NFC", text or "")


def _feature_hash(gram: str) -> int:
    value = 2166136261
    for char in gram:
        value = ((value ^ ord(char)) * 16777619) & 0xFFFFFFFF
    return value & FEATURE_MASK


def feature_ids(text: str) -> set[int]:
    """Return unique hashed 2–5 character word n-grams for one document."""
    result: set[int] = set()
    for token in TOKEN_RE.findall(_canonical(text).casefold()[:4000]):
        if len(token) > 40:
            token = token[:40]
        padded = "^" + token + "$"
        for n in range(2, 6):
            for start in range(len(padded) - n + 1):
                result.add(_feature_hash(padded[start:start + n]))
    return result


def _text_digest(text: str) -> bytes:
    canonical = " ".join(_canonical(text).split())
    return hashlib.sha256(canonical.encode("utf-8")).digest()


def _offer_sample(heap: list[tuple[int, str, str]], seen: set[str], text: str,
                  digest: bytes, limit: int) -> None:
    digest_hex = digest.hex()
    if digest_hex in seen:
        return
    rank = int.from_bytes(hashlib.sha256(b"kmr-zazaki-sample-v1" + digest).digest()[:8], "big")
    item = (-rank, digest_hex, text[:4000])
    if len(heap) < limit:
        heapq.heappush(heap, item)
        seen.add(digest_hex)
    elif rank < -heap[0][0]:
        removed = heapq.heapreplace(heap, item)
        seen.remove(removed[1])
        seen.add(digest_hex)


def _select_samples(source: Path, train_cap: int, validation_cap: int,
                    min_probability: float) -> tuple[dict[str, list[str]], dict[str, int]]:
    labels = {"kmr_Latn": "kmr_Latn", "diq_Latn": "zazaki", "kiu_Latn": "zazaki"}
    train_heaps = {"kmr_Latn": [], "zazaki": []}
    validation_heaps = {"kmr_Latn": [], "zazaki": []}
    train_seen = {"kmr_Latn": set(), "zazaki": set()}
    validation_seen = {"kmr_Latn": set(), "zazaki": set()}
    counts = {"kmr_Latn": 0, "zazaki": 0, "ignored_or_low_confidence": 0}
    with source.open("rb") as stream:
        for line_number, raw in enumerate(stream, 1):
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                counts["ignored_or_low_confidence"] += 1
                continue
            label = labels.get(row.get("lid_label"))
            try:
                probability = float(row.get("lid_prob", 0.0))
            except (TypeError, ValueError):
                probability = 0.0
            text = row.get("text")
            if label is None or probability < min_probability or not isinstance(text, str) or len(text.strip()) < 120:
                counts["ignored_or_low_confidence"] += 1
                continue
            digest = _text_digest(text)
            counts[label] += 1
            # Exact duplicates stay in the same split, so no sampled text can leak.
            validation = digest[0] % 5 == 0
            if validation:
                _offer_sample(validation_heaps[label], validation_seen[label], text, digest, validation_cap)
            else:
                _offer_sample(train_heaps[label], train_seen[label], text, digest, train_cap)
            if line_number % 250_000 == 0:
                print(f"scanned {line_number:,} rows", file=sys.stderr, flush=True)

    samples: dict[str, list[str]] = {}
    for label in ("kmr_Latn", "zazaki"):
        samples[f"train:{label}"] = [item[2] for item in train_heaps[label]]
        samples[f"validation:{label}"] = [item[2] for item in validation_heaps[label]]
    return samples, counts


def _fit(samples: dict[str, list[str]], alpha: float = 0.25) -> tuple[float, dict[int, float]]:
    dimensions = 1 << FEATURE_BITS
    kmr_counts = [0] * dimensions
    zazaki_counts = [0] * dimensions
    kmr_docs = len(samples["train:kmr_Latn"])
    zazaki_docs = len(samples["train:zazaki"])
    for label, counts in (("kmr_Latn", kmr_counts), ("zazaki", zazaki_counts)):
        texts = samples[f"train:{label}"]
        for index, text in enumerate(texts, 1):
            for feature in feature_ids(text):
                counts[feature] += 1
            if index % 500 == 0:
                print(f"featurized {label}: {index:,}/{len(texts):,}", file=sys.stderr, flush=True)

    # Bernoulli Naive Bayes: score all absent features as a shared baseline,
    # then store sparse adjustments only for features present in the document.
    base = math.log(kmr_docs / zazaki_docs)
    weights: dict[int, float] = {}
    for feature in range(dimensions):
        p_kmr = (kmr_counts[feature] + alpha) / (kmr_docs + 2 * alpha)
        p_zazaki = (zazaki_counts[feature] + alpha) / (zazaki_docs + 2 * alpha)
        absent = math.log1p(-p_kmr) - math.log1p(-p_zazaki)
        base += absent
        present_adjustment = math.log(p_kmr) - math.log(p_zazaki) - absent
        if abs(present_adjustment) > 1e-12:
            weights[feature] = present_adjustment
    return base, weights


def score(text: str, model: dict[str, Any]) -> float:
    value = float(model["base_log_odds"])
    weights = model["weights"]
    for feature in sorted(feature_ids(text)):
        value += weights.get(str(feature), 0.0)
    return value


def _precision_threshold(scores: list[tuple[float, bool]], positive: bool,
                         target: float, min_predictions: int) -> dict[str, Any]:
    if positive:
        ordered = sorted(scores, key=lambda item: item[0], reverse=True)
    else:
        ordered = sorted(scores, key=lambda item: item[0])
    predicted = correct = 0
    selected: tuple[float, int, int] | None = None
    i = 0
    while i < len(ordered):
        threshold = ordered[i][0]
        j = i
        while j < len(ordered) and ordered[j][0] == threshold:
            predicted += 1
            is_positive = ordered[j][1]
            if (is_positive if positive else not is_positive):
                correct += 1
            j += 1
        if predicted >= min_predictions and correct / predicted >= target:
            selected = (threshold, correct, predicted)
        i = j
    if selected is None:
        return {"threshold": None, "precision": None, "coverage": 0, "predictions": 0}
    threshold, good, total = selected
    return {"threshold": threshold, "precision": good / total, "coverage": total / len(scores), "predictions": total}


def train(source: str | os.PathLike[str], output: str | os.PathLike[str], *,
          train_cap: int = TRAIN_PER_CLASS, validation_cap: int = VALIDATION_PER_CLASS,
          min_probability: float = 0.98, target_precision: float = TARGET_PRECISION) -> dict[str, Any]:
    source_path = Path(source)
    samples, observed = _select_samples(source_path, train_cap, validation_cap, min_probability)
    sizes = {key: len(value) for key, value in samples.items()}
    needed = {"train:kmr_Latn": MIN_TRAIN, "train:zazaki": MIN_TRAIN,
              "validation:kmr_Latn": MIN_VALIDATION, "validation:zazaki": MIN_VALIDATION}
    short = {key: (sizes[key], minimum) for key, minimum in needed.items() if sizes[key] < minimum}
    if short:
        raise RuntimeError(f"Not enough distinct high-confidence labeled rows for both classes: {short}; observed labels={observed}")

    base, weights = _fit(samples)
    holdout: list[tuple[float, bool]] = []
    scoring_model = {"base_log_odds": base, "weights": {str(k): v for k, v in weights.items()}}
    for label, is_kmr in (("kmr_Latn", True), ("zazaki", False)):
        for text in samples[f"validation:{label}"]:
            holdout.append((score(text, scoring_model), is_kmr))
    raw_correct = sum((value >= 0) == is_kmr for value, is_kmr in holdout)
    raw_metrics = {"accuracy": raw_correct / len(holdout), "examples": len(holdout)}
    thresholds = {
        "kmr": _precision_threshold(holdout, True, target_precision, MIN_CONFIDENT_PREDICTIONS),
        "zazaki": _precision_threshold(holdout, False, target_precision, MIN_CONFIDENT_PREDICTIONS),
    }
    if not any(thresholds[key]["threshold"] is not None for key in thresholds):
        raise RuntimeError("Held-out samples do not support either confident class at the required precision; no model saved")

    model: dict[str, Any] = {
        "version": MODEL_VERSION,
        "algorithm": "character-ngram-bernoulli-nb-hashed",
        "feature_bits": FEATURE_BITS,
        "classes": {"kmr": "kmr_Latn", "zazaki": sorted(ZAZAKI_LABELS)},
        "minimum_text_chars": 100,
        "base_log_odds": base,
        "weights": {str(k): v for k, v in weights.items()},
        "thresholds": thresholds,
        "training": {
            "source": "kurdish-tech/KurdishCorpus-clean full JSONL",
            "labels": {"kmr": "kmr_Latn", "zazaki": sorted(ZAZAKI_LABELS)},
            "minimum_lid_probability": min_probability,
            "target_heldout_precision": target_precision,
            "sample_caps_per_class": {"train": train_cap, "validation": validation_cap},
            "sample_sizes": sizes,
            "observed_high_confidence_rows": observed,
            "heldout_raw_metrics": raw_metrics,
        },
    }
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp.write_text(json.dumps(model, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, output_path)
    return model


def load_model(path: str | os.PathLike[str]) -> dict[str, Any]:
    model = json.loads(Path(path).read_text(encoding="utf-8"))
    if model.get("version") != MODEL_VERSION or model.get("algorithm") != "character-ngram-bernoulli-nb-hashed":
        raise ValueError("Unsupported Kurmanji/Zazaki model format")
    return model


def classify(text: str, model: dict[str, Any]) -> dict[str, Any]:
    """Classify confidently or abstain. Labels are kmr_Latn, zazaki, unknown."""
    normalized = _canonical(text).strip()
    if len(normalized) < int(model.get("minimum_text_chars", 100)) or len(TOKEN_RE.findall(normalized)) < 12:
        return {"label": "unknown", "score": None, "reason": "insufficient_text"}
    value = score(normalized, model)
    positive = model["thresholds"]["kmr"]["threshold"]
    negative = model["thresholds"]["zazaki"]["threshold"]
    if positive is not None and value >= float(positive):
        return {"label": "kmr_Latn", "score": value, "reason": "heldout_precision_threshold"}
    if negative is not None and value <= float(negative):
        return {"label": "zazaki", "score": value, "reason": "heldout_precision_threshold"}
    return {"label": "unknown", "score": value, "reason": "ambiguous"}


def classify_kurdish(text: str, model: dict[str, Any]) -> dict[str, Any]:
    """Compatibility alias for crawler integrations."""
    return classify(text, model)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    train_parser = subparsers.add_parser("train", help="train from the complete local HF JSONL")
    default_data = Path(os.environ.get("DATA_DIR", "data")) / "corpus_v2_open_release.jsonl"
    train_parser.add_argument("--input", type=Path, default=default_data)
    train_parser.add_argument("--output", type=Path,
                              default=Path(os.environ.get("DATA_DIR", "data")) / "kmr_zazaki_ngram.json")
    train_parser.add_argument("--train-per-class", type=int, default=TRAIN_PER_CLASS)
    train_parser.add_argument("--validation-per-class", type=int, default=VALIDATION_PER_CLASS)
    train_parser.add_argument("--min-lid-probability", type=float, default=0.98)
    train_parser.add_argument("--target-precision", type=float, default=TARGET_PRECISION)
    classify_parser = subparsers.add_parser("classify", help="classify one text file")
    classify_parser.add_argument("--model", type=Path, required=True)
    classify_parser.add_argument("--text-file", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "train":
        model = train(args.input, args.output, train_cap=args.train_per_class,
                      validation_cap=args.validation_per_class,
                      min_probability=args.min_lid_probability, target_precision=args.target_precision)
        print(json.dumps({"output": str(args.output), "sizes": model["training"]["sample_sizes"],
                          "heldout": model["training"]["heldout_raw_metrics"],
                          "thresholds": model["thresholds"]}, indent=2))
    else:
        model = load_model(args.model)
        text = args.text_file.read_text(encoding="utf-8")
        print(json.dumps(classify(text, model), ensure_ascii=False))


if __name__ == "__main__":
    main()
