"""
Schema security tests — proves Pydantic rejects malformed input at the boundary.
Run: pytest tests/test_schema.py -v
"""

import pytest
from pydantic import ValidationError

# Adjust import path to match your project structure:
# from src.engine.api.schema import CompleteSongIntent, StructureSection, TrackIntent
from schema import CompleteSongIntent, StructureSection, TrackIntent


# ─── Fixtures ──────────────────────────────────────────────────────────────────

VALID_INTENT = {
    "core_desire": "[WOUND] Finding someone I loved after they chose to leave",
    "mood_primary": "Grief",
    "genre": "Lo-Fi Bedroom",
    "tempo": 75,
    "key_mode": "F major",
    "structure": [{"name": "verse", "bars": 8, "repetitions": 2}],
    "instruments": [{"instrument": "piano", "techniques": ["arpeggiate"]}],
    "allow_legacy_fallback": False,
}


def make_intent(**overrides):
    """Helper: create valid intent with selective overrides."""
    data = {**VALID_INTENT, **overrides}
    return CompleteSongIntent(**data)


# ─── Happy path ────────────────────────────────────────────────────────────────

class TestValidIntents:
    def test_canonical_intent(self):
        intent = make_intent()
        assert intent.tempo == 75
        assert intent.key_mode == "F major"

    def test_all_modes(self):
        for m in ["major", "minor", "dorian", "mixolydian", "lydian", "phrygian", "locrian"]:
            intent = make_intent(key_mode=f"C {m}")
            assert intent.key_mode == f"C {m}"

    def test_sharp_flat_keys(self):
        for k in ["C# minor", "Db major", "F# dorian", "Bb lydian", "Ab phrygian"]:
            intent = make_intent(key_mode=k)
            assert intent.key_mode == k

    def test_instrument_normalization(self):
        intent = make_intent(instruments=[{"instrument": "  Synth Bass  ", "techniques": []}])
        assert intent.instruments[0].instrument == "synth_bass"

    def test_technique_dedup(self):
        intent = make_intent(instruments=[{"instrument": "piano", "techniques": ["legato", "legato", "staccato"]}])
        assert intent.instruments[0].techniques == ["legato", "staccato"]

    def test_max_tempo(self):
        intent = make_intent(tempo=300)
        assert intent.tempo == 300

    def test_min_tempo(self):
        intent = make_intent(tempo=40)
        assert intent.tempo == 40


# ─── Rejection tests ──────────────────────────────────────────────────────────

class TestSchemaRejections:
    def test_empty_core_desire(self):
        with pytest.raises(ValidationError, match="core_desire"):
            make_intent(core_desire="")

    def test_oversized_core_desire(self):
        with pytest.raises(ValidationError, match="core_desire"):
            make_intent(core_desire="x" * 1001)

    def test_empty_mood(self):
        with pytest.raises(ValidationError, match="mood_primary"):
            make_intent(mood_primary="")

    def test_empty_genre(self):
        with pytest.raises(ValidationError, match="genre"):
            make_intent(genre="")

    def test_tempo_too_low(self):
        with pytest.raises(ValidationError, match="tempo"):
            make_intent(tempo=39)

    def test_tempo_too_high(self):
        with pytest.raises(ValidationError, match="tempo"):
            make_intent(tempo=301)

    def test_invalid_key_mode_no_space(self):
        with pytest.raises(ValidationError, match="key_mode"):
            make_intent(key_mode="Cmajor")

    def test_invalid_key_mode_bad_note(self):
        with pytest.raises(ValidationError, match="key_mode"):
            make_intent(key_mode="H major")

    def test_invalid_key_mode_bad_mode(self):
        with pytest.raises(ValidationError, match="key_mode"):
            make_intent(key_mode="C blues")

    def test_invalid_section_name(self):
        with pytest.raises(ValidationError, match="section name"):
            make_intent(structure=[{"name": "breakdown", "bars": 8}])

    def test_bars_too_high(self):
        with pytest.raises(ValidationError, match="bars"):
            make_intent(structure=[{"name": "verse", "bars": 129}])

    def test_total_bars_overflow(self):
        with pytest.raises(ValidationError, match="1000"):
            make_intent(structure=[{"name": "verse", "bars": 128, "repetitions": 16}])

    def test_empty_structure(self):
        with pytest.raises(ValidationError, match="structure"):
            make_intent(structure=[])

    def test_empty_instruments(self):
        with pytest.raises(ValidationError, match="instruments"):
            make_intent(instruments=[])

    def test_empty_instrument_name(self):
        with pytest.raises(ValidationError, match="instrument"):
            make_intent(instruments=[{"instrument": "", "techniques": []}])

    def test_too_many_instruments(self):
        with pytest.raises(ValidationError, match="32"):
            make_intent(instruments=[{"instrument": f"inst_{i}", "techniques": []} for i in range(33)])

    def test_reps_too_high(self):
        with pytest.raises(ValidationError, match="repetitions"):
            make_intent(structure=[{"name": "verse", "bars": 8, "repetitions": 17}])


# ─── Edge cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_exactly_1000_bars(self):
        """Boundary: exactly at limit should pass."""
        intent = make_intent(structure=[{"name": "verse", "bars": 100, "repetitions": 10}])
        assert sum(s.bars * s.repetitions for s in intent.structure) == 1000

    def test_1001_bars_rejected(self):
        """Boundary: one over should fail."""
        with pytest.raises(ValidationError):
            make_intent(structure=[
                {"name": "verse", "bars": 100, "repetitions": 10},
                {"name": "outro", "bars": 1, "repetitions": 1},
            ])

    def test_whitespace_instrument_stripped(self):
        intent = make_intent(instruments=[{"instrument": "  piano  "}])
        assert intent.instruments[0].instrument == "piano"

    def test_key_mode_whitespace_stripped(self):
        intent = make_intent(key_mode="  C major  ")
        assert intent.key_mode == "C major"
