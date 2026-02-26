# TOTaLi-Assisted Drafting Pipeline

**Defensible Spatial Drafting Pipeline**

- AI Classifies (probabilistic, non-authoritative)
- Algorithms Measure (deterministic computational geometry)
- Humans Certify (PLS remains sovereign)

## Pipeline Phases

1. **Geodetic Gatekeeper** – CRS/epoch/unit validation, PROJ transformations
2. **ML Segmentation** – LiDAR semantic classification (ground, curb, wire, building, etc.)
3. **Deterministic Extraction** – DTM/TIN, breaklines, contours, planimetric vectors
4. **CAD Shielding** – Middleware isolation, geometry healing/quarantine
5. **Surveyor Linting** – Ghost suggestions in CAD, human accept/reject, audit logging

## Install

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Run full pipeline
python -m totali.main --input path/to/pointcloud.las --config config/pipeline.yaml

# Run individual phases
python -m totali.main --input data.las --phase geodetic
python -m totali.main --input data.las --phase segment
python -m totali.main --input data.las --phase extract
```

## Architecture

```
AI output → DRAFT layer only → Human review → Accept/Reject → Certified layer
                                                    ↓
                                              Audit log (chain of custody)
```

No geometry is ever auto-promoted to certified status.
